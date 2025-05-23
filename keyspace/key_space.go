package keyspace

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"path"
	"sort"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/mirror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A KeySpace is a local mirror of a decoded portion of the Etcd key/value space,
// which is kept in sync with Etcd via long-lived Watch operations. KeySpace
// must be read-locked before access, to guard against concurrent updates.
type KeySpace struct {
	// Key prefix which roots this KeySpace.
	Root string
	// Header is the last Etcd operation revision which actually updated this KeySpace:
	// in other words, `Header.Revision` is the greatest `ModRevision` of any key under
	// the `Root`. It is not incremented by other Etcd operations which don't actually
	// change the KeySpace, such as progress notifications. This property means, for
	// all watched KeySpaces having a common Root, their revisions are always directly
	// comparable and will be equal if (and only if) they reflect the same keys and
	// values.
	Header etcdserverpb.ResponseHeader
	// KeyValues is a complete and decoded mirror of the (prefixed) Etcd key/value space.
	KeyValues
	// Observers called upon each mutation of the KeySpace. Observer calls occur
	// in-order, after the KeySpace itself has been updated, and while a write-
	// lock over the KeySpace is still held (which Observers must not release).
	// Observers is intended to simplify atomicity of stateful representations
	// deriving themselves from the KeySpace: any mutations performed by Observers
	// will appear synchronously with changes to the KeySpace itself, from the
	// perspective of a client appropriately utilizing a read-lock.
	Observers []func()
	// WatchApplyDelay is the duration for which KeySpace should allow Etcd
	// WatchResponses to queue before applying all responses to the KeySpace.
	// This Nagle-like mechanism amortizes the cost of applying many
	// WatchResponses arriving in close succession. Default is 30ms.
	WatchApplyDelay time.Duration
	// Mu guards Header, KeyValues, and Observers. It must be locked before any are accessed.
	Mu sync.RWMutex

	decode   KeyValueDecoder // Client-provided KeySpace decoder.
	next     KeyValues       // Reusable buffer for next, amortized KeyValues update.
	updateCh chan struct{}   // Signals waiting goroutines of an update.
}

// NewKeySpace returns a KeySpace with the configured key |prefix| and |decoder|.
// |prefix| must be a "Clean" path, as defined by path.Clean, or NewKeySpace panics.
// This check limits the space of possible prefixes somewhat, but guards against
// many common and unintentional errors (eg, mixed use of trailing slashes).
func NewKeySpace(prefix string, decoder KeyValueDecoder) *KeySpace {
	if c := path.Clean(prefix); c != prefix {
		panic(fmt.Sprintf("expected prefix to be a cleaned path (%s != %s)", c, prefix))
	}
	var ks = &KeySpace{
		Root:            prefix,
		WatchApplyDelay: 30 * time.Millisecond,
		decode:          decoder,
		updateCh:        make(chan struct{}),
	}
	return ks
}

// Load loads a snapshot of the prefixed KeySpace at revision |rev|,
// or if |rev| is zero, at the current revision.
func (ks *KeySpace) Load(ctx context.Context, client *clientv3.Client, rev int64) error {
	if rev == 0 {
		// Resolve a current Revision. Note |rev| of zero is also interpreted by
		// SyncBase as "use a recent revision", which we would use instead except
		// there's no way to extract what that Revision actually was...
		if resp, err := client.Get(ctx, "a-key-we-don't-expect-to-exist"); err != nil {
			return err
		} else {
			rev = resp.Header.Revision
		}
	}
	defer ks.Mu.Unlock()
	ks.Mu.Lock()

	ks.Header, ks.KeyValues = etcdserverpb.ResponseHeader{}, ks.KeyValues[:0]
	var respCh, errCh = mirror.NewSyncer(client, ks.Root, rev).SyncBase(ctx)

	// Read messages across |respCh| and |errCh| until both are closed.
	for respCh != nil || errCh != nil {
		select {
		case resp, ok := <-respCh:
			if !ok {
				respCh = nil // Finished draining |respCh|.
			} else if err := checkHeader(&ks.Header, *resp.Header); err != nil {
				return err
			} else {
				ks.Header = *resp.Header
				for _, kv := range resp.Kvs {
					if ks.KeyValues, err = appendKeyValue(ks.KeyValues, ks.decode, kv); err != nil {
						log.WithFields(log.Fields{"key": string(kv.Key), "err": err}).
							Error("key/value decode failed while loading")
					}
				}
			}
		case err, ok := <-errCh:
			if !ok {
				errCh = nil // Finished draining |errCh|.
			} else {
				return err
			}
		}
	}
	// Etcd defines `ResponseHeader.Revision` to be the store revision when the
	// request was applied (and importantly, not of the revision of the request).
	// We deviate from this and record the requested revision. In other words, we
	// maintain our Header as the effective Revision of the KeySpace.
	ks.Header.Revision = rev

	ks.onUpdate()
	return nil
}

// Watch a loaded KeySpace and apply updates as they are received.
func (ks *KeySpace) Watch(ctx context.Context, client clientv3.Watcher) error {
	var watchCh clientv3.WatchChan

	// WatchResponses can often arrive in quick succession and contain many key
	// events. We amortize the cost of updating the KeySpace by briefly delaying
	// the application of a first WatchResponse to await further events, and then
	// applying all events as a single bulk update.
	var responses []clientv3.WatchResponse
	var applyTimer = time.NewTimer(0)
	<-applyTimer.C // Now idle.

	// TODO(johnny): this lock guards a current race in scenarios_test.go.
	ks.Mu.RLock()
	// resumeRevision is the point at which we'll resume watch operations.
	// This is separate from the `ks.Header.Revision`, which tracks the latest
	// revision that's actually modified the KeySpace. ProgressNotify responses
	// should update the `resumeRevision`, but _not_ the `ks.Header.Revision`.
	var resumeRevision = ks.Header.Revision + 1
	ks.Mu.RUnlock()

	for attempt := 0; true; attempt++ {
		// Start (or restart) a new long-lived Watch. Note this is very similar to
		// mirror.Syncer: A key difference (and the reason that API is not used) is
		// the additional WithProgressNotify option - without this option, in the
		// event of a long-lived watch over a prefix which has no key changes, Etcd
		// may compact away the watch revision and a retried Watch will later fail.
		// WithProgressNotify ensures the watched revision is kept reasonably recent
		// even if no WatchResponses otherwise arrive.
		//
		// The "require leader" option is also set on the watch context. With this
		// setting, if our watched Etcd instance is partitioned from its majority
		// it will abort our watch stream rather than wait for the partition to
		// resolve (the default). We'll then retry against another endpoint in the
		// majority. Without this setting, there's a potential deadlock case if our
		// watch Etcd is partitioned from the majority, but *we* are not and can
		// keep our lease alive. Specifically this can lead to our being allocator
		// leader but being unable to observe updates.

		if watchCh == nil {
			watchCh = client.Watch(clientv3.WithRequireLeader(ctx), ks.Root,
				clientv3.WithPrefix(),
				clientv3.WithProgressNotify(),
				clientv3.WithRev(resumeRevision),
			)
		}

		// Queue a new WatchResponse or, if |applyTimer| has fired, apply previously
		// queued responses. Go's uniform psuedo-random selection among select cases
		// prevents starvation.
		select {
		case resp, ok := <-watchCh:
			if !ok {
				return ctx.Err() // Watch contract implies the context is cancelled.
			} else if err := resp.Err(); err != nil && !resp.Canceled {
				log.WithFields(log.Fields{"err": err, "attempt": attempt}).
					Warn("non-terminal watch error (will continue)")
			} else if status, _ := status.FromError(err); err == rpctypes.ErrNoLeader || status.Code() == codes.Unknown {
				// ErrNoLeader indicates our watched Etcd is in a partitioned minority.
				// Unknown gRPC status can happen during Etcd shutdowns, such as in:
				//
				//   rpc error: code = Unknown desc = malformed header: missing HTTP content-type
				//
				// In both cases we restart the watch.
				watchCh = nil

				log.WithFields(log.Fields{"err": err, "attempt": attempt}).
					Warn("watch channel failed (will restart)")

				select {
				case <-time.After(backoff(attempt)): // Pass.
				case <-ctx.Done():
					return ctx.Err()
				}
			} else if err != nil {
				return err // All other errors are fatal.
			} else if len(resp.Events) > 0 {
				log.WithFields(log.Fields{
					"header":          resp.Header,
					"compactRevision": resp.CompactRevision,
					"numEvents":       len(resp.Events),
					"watchIter":       attempt,
					"minModRevision":  resp.Events[0].Kv.ModRevision,
					"maxModRevision":  resp.Events[len(resp.Events)-1].Kv.ModRevision,
				}).Trace("received etcd watch response")

				// Update resumeRevision to be the greatest observed ModRevision
				// Events are ordered by ModRevision, so the last is the max.
				resumeRevision = resp.Events[len(resp.Events)-1].Kv.ModRevision + 1
				if len(responses) == 0 {
					applyTimer.Reset(ks.WatchApplyDelay)
				}
				responses = append(responses, resp)
				attempt = 0 // Restart sequence.
			} else if resp.IsProgressNotify() {
				log.WithFields(log.Fields{
					"header":             resp.Header,
					"compactRevision":    resp.CompactRevision,
					"watchIter":          attempt,
					"prevResumeRevision": resumeRevision,
				}).Debug("received watch progress notification")
				// A subsequent watch should start at the next revision
				resumeRevision = resp.Header.Revision + 1
			} else {
				log.WithFields(log.Fields{
					"watchIter":      attempt,
					"resumeRevision": resumeRevision,
					"response":       resp,
				}).Warn("ignoring unexpected empty watch response")
			}
		case <-applyTimer.C:
			// Apply all previously buffered WatchResponses. |applyTimer| is now
			// idle and will remain so until the next response is received.
			if err := ks.Apply(responses...); err != nil {
				return err
			}
			responses = responses[:0]
		}
	}
	panic("not reached")
}

// Update returns a channel which will signal on the next KeySpace update.
// It's not required to hold a read lock of the KeySpace Mutex, and a write
// lock must not be held or Update will deadlock.
func (ks *KeySpace) Update() <-chan struct{} {
	ks.Mu.RLock()
	defer ks.Mu.RUnlock()

	return ks.updateCh
}

// WaitForRevision blocks until the KeySpace Revision is at least |revision|,
// or until the context is done. A read lock of the KeySpace Mutex must be
// held at invocation, and will be re-acquired before WaitForRevision returns.
func (ks *KeySpace) WaitForRevision(ctx context.Context, revision int64) error {
	for {
		if err := ctx.Err(); err != nil || ks.Header.Revision >= revision {
			// Return current context error even if we also saw the revision,
			// to disambiguate cases where the KeySpace appears inconsistent
			// due to a cancellation of our context (eg, our allocator member
			// key is missing because our lease was cancelled).
			return err
		}

		var ch = ks.updateCh

		ks.Mu.RUnlock()
		select {
		case <-ch:
		case <-ctx.Done():
		}
		ks.Mu.RLock()
	}
}

// Apply one or more Etcd WatchResponses to the KeySpace. Apply returns only
// unrecoverable Application errors; inconsistencies in the updates themselves
// are instead logged. Apply is exported principally in support of testing
// fixtures; most clients should instead use Watch. Clients must ensure
// concurrent calls to Apply are not made.
func (ks *KeySpace) Apply(responses ...clientv3.WatchResponse) error {
	var wr clientv3.WatchResponse

	// This will become the new KeySpace.Header after we're done applying all the responses.
	// We'll validate each response header to make sure that it's consistent with this one,
	// but we'll only update this header with `ModRevision` of the events within the response.
	var nextHeader = ks.Header

	for _, wr = range responses {
		if err := checkHeader(&nextHeader, wr.Header); err != nil {
			return err
		}
		// Sanity check that the earliest ModRevision in each response is greater than the
		// greatest ModRevision in the previous response, and that the last ModRevision is
		// at least as large as the first ModRevision (they should be monotonic).
		// This is all guaranteed by ETCD, but this is just making that explicit.
		if firstRevision := wr.Events[0].Kv.ModRevision; firstRevision <= nextHeader.Revision {
			return fmt.Errorf("received watch response with first ModRevision %d, which is <= last Revision %d",
				firstRevision, nextHeader.Revision)
		} else if lastRevision := wr.Events[len(wr.Events)-1].Kv.ModRevision; lastRevision < firstRevision {
			return fmt.Errorf("received watch response with last ModRevision %d less than first ModRevision %d",
				lastRevision, firstRevision)
		} else {
			nextHeader.Revision = lastRevision
			// If MemberId or RaftTerm have changed, it would have already been logged by `checkHeader`.
			nextHeader.MemberId = wr.Header.MemberId
			nextHeader.RaftTerm = wr.Header.RaftTerm
		}

		// Order on key, while preserving relative ModRevision order of events of the same key.
		sort.SliceStable(wr.Events, func(i, j int) bool {
			return bytes.Compare(wr.Events[i].Kv.Key, wr.Events[j].Kv.Key) < 0
		})
	}

	// Heap WatchResponses on (Key, ModRevision) order of the first response Event.
	var responseHeap = watchResponseHeap(responses)
	heap.Init(&responseHeap)

	// We'll build a new KeyValues into |next| via a single iteration over the
	// key space. Unmodified runs of keys are copied from |current|, with
	// Watch Events applied as they are encountered and in (Key, ModRevision) order.
	var current, next = ks.KeyValues, ks.next

	for responseHeap.Len() != 0 {
		if wr = heap.Pop(&responseHeap).(clientv3.WatchResponse); len(wr.Events) == 0 {
			continue
		}
		// wr.Events[0] is the next Event in the overall sequence ordered on (Key, ModRevision).

		// Find the |ind| of this key in |current|. Copy and append all preceding
		// keys to |next|, through a current value for this key. Then step |current|
		// forward, marking this range as completed.
		var ind, found = current.Search(string(wr.Events[0].Kv.Key))
		if found {
			ind++
		}
		next, current = append(next, current[:ind]...), current[ind:]

		// Patch the tail of |next|, inserting, modifying, or deleting at the last element.
		var err error
		if next, err = updateKeyValuesTail(next, ks.decode, *wr.Events[0]); err != nil {
			log.WithFields(log.Fields{"err": err, "event": wr.Events[0].Kv.String()}).
				Error("inconsistent watched key/value event")
		}

		// Pop wr.Events[0], and re-order the next Event in the heap.
		wr.Events = wr.Events[1:]
		heap.Push(&responseHeap, wr)
	}
	next = append(next, current...) // Append any left-over elements in |current|.

	// Critical section: update header, swap out rebuilt KeyValues, and notify observers.
	ks.Mu.Lock()
	ks.Header = nextHeader
	ks.KeyValues, ks.next = next, ks.KeyValues[:0]
	ks.onUpdate()
	ks.Mu.Unlock()

	return nil
}

func (ks *KeySpace) onUpdate() {
	for _, obv := range ks.Observers {
		obv()
	}
	close(ks.updateCh)
	ks.updateCh = make(chan struct{})
}

// checkHeader returns an error if the ClusterIds are not the same, or if the Revision of `update` is less than the
// Revision of `h`.
func checkHeader(h *etcdserverpb.ResponseHeader, update etcdserverpb.ResponseHeader) error {
	if h.ClusterId != 0 && h.ClusterId != update.ClusterId {
		return fmt.Errorf("etcd ClusterID mismatch (expected %d, got %d)", h.ClusterId, update.ClusterId)
	} else if update.Revision < h.Revision {
		return fmt.Errorf("etcd Revision mismatch (expected >= %d, got %d)", h.Revision, update.Revision)
	}

	if h.ClusterId != 0 && (h.MemberId != update.MemberId || h.RaftTerm != update.RaftTerm) {
		log.WithFields(log.Fields{
			"memberId":        h.MemberId,
			"raftTerm":        h.RaftTerm,
			"update.MemberId": update.MemberId,
			"update.RaftTerm": update.RaftTerm,
		}).Info("etcd MemberId/RaftTerm changed")
	}
	return nil
}

// watchResponseHeap implements heap.Interface over WatchResponses.
type watchResponseHeap []clientv3.WatchResponse

func (h watchResponseHeap) Len() int            { return len(h) }
func (h watchResponseHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *watchResponseHeap) Push(x interface{}) { *h = append(*h, x.(clientv3.WatchResponse)) }

// Less orders on ascending key, and for the same key, ascending ModRevision.
func (h watchResponseHeap) Less(i, j int) bool {
	if li, lj := len(h[i].Events), len(h[j].Events); li == 0 && lj == 0 {
		return false
	} else if li == 0 {
		return true
	} else if lj == 0 {
		return false
	} else if c := bytes.Compare(h[i].Events[0].Kv.Key, h[j].Events[0].Kv.Key); c != 0 {
		return c < 0
	} else {
		return h[i].Events[0].Kv.ModRevision < h[j].Events[0].Kv.ModRevision
	}
}

func (h *watchResponseHeap) Pop() interface{} {
	var old, n = *h, len(*h)
	var x = old[n-1]
	*h = old[0 : n-1]
	return x
}

func backoff(attempt int) time.Duration {
	switch attempt {
	case 0, 1:
		return 0
	case 2:
		return time.Millisecond * 5
	case 3, 4, 5:
		return time.Second * time.Duration(attempt-1)
	default:
		return 5 * time.Second
	}
}
