package fragment

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/stores"
	"golang.org/x/net/trace"
)

const (
	// When a covering fragment cannot be found, we allow serving a *greater*
	// fragment so long as it was last modified at least this long ago.
	offsetJumpAgeThreshold = 6 * time.Hour
)

// Index maintains a queryable index of local and remote journal Fragments.
type Index struct {
	ctx            context.Context // Context over the lifetime of the Index.
	set            CoverSet        // All Fragments of the index (local and remote).
	local          CoverSet        // Local Fragments only (having non-nil File).
	condCh         chan struct{}   // Condition variable; notifies blocked queries on each |set| update.
	firstRefreshCh chan struct{}   // Closed when the first remote index load has completed.
	mu             sync.RWMutex    // Guards |set| and |condCh|.
}

// NewIndex returns a new, empty Index.
func NewIndex(ctx context.Context) *Index {
	return &Index{
		ctx:            ctx,
		condCh:         make(chan struct{}),
		firstRefreshCh: make(chan struct{}),
	}
}

// Query the Index for a Fragment matching the ReadRequest.
func (fi *Index) Query(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, File, error) {
	defer fi.mu.RUnlock()
	fi.mu.RLock()

	var resp = &pb.ReadResponse{
		Offset: req.Offset,
	}

	// Special handling for reads at the Journal Write head.
	if resp.Offset == -1 {
		resp.Offset = fi.set.EndOffset()
	}

	for {
		var ind, found = fi.set.LongestOverlappingFragment(resp.Offset)

		var condCh = fi.condCh
		var err error

		// If the requested offset isn't covered by the index, but we do have
		// a persisted fragment with a *greater* offset...
		if !found && ind != len(fi.set) && fi.set[ind].ModTime != 0 &&
			// AND the client is reading from the very beginning of the available journal,
			// OR the next available fragment was persisted quite a while ago.
			(ind == 0 || (fi.set[ind].ModTime < timeNow().Add(-offsetJumpAgeThreshold).Unix())) {

			// Then skip the read forward to the first or next available offset.
			// This case allows us to recover from "holes" or deletions in the
			// offset space of a Journal, while not impacting races which can occur
			// between delayed persistence to the Fragment store vs hand-off of
			// Journals to new brokers (eg, a new broker which isn't yet aware of
			// a Fragment currently being uploaded, should block a read
			// of an offset covered by that Fragment until it becomes available).
			resp.Offset = fi.set[ind].Begin
			found = true
		}

		if !found {
			// Pass.
		} else if f := fi.set[ind].Fragment; f.ModTime != 0 && f.ModTime < req.BeginModTime {
			// This fragment was modified before the requested lower bound.
			// Skip the read offset over its content.
			addTrace(ctx, "Index.Query(%s) => skip offsets [%d, %d) because ModTime %d < BeginModTime %d",
				req, f.Begin, f.End, f.ModTime, req.BeginModTime)

			resp.Offset = fi.set[ind].End
			continue
		} else {
			// We found a covering fragment.
			resp.Status = pb.Status_OK
			resp.WriteHead = fi.set.EndOffset()
			resp.Fragment = new(pb.Fragment)
			*resp.Fragment = fi.set[ind].Fragment

			if resp.Fragment.BackingStore != "" && resp.Fragment.ModTime != 0 {
				resp.FragmentUrl, err = SignGetURL(*resp.Fragment, time.Minute)
			}
			addTrace(ctx, "Index.Query(%s) => %s, localFile: %t", req, resp, fi.set[ind].File != nil)
			return resp, fi.set[ind].File, err
		}

		if !req.Block {
			resp.Status = pb.Status_OFFSET_NOT_YET_AVAILABLE
			resp.WriteHead = fi.set.EndOffset()

			addTrace(ctx, "Index.Query(%s) => %s", req, resp)
			return resp, nil, nil
		}

		addTrace(ctx, " ... stalled in Index.Query(%s)", req)

		// Wait for |condCh| to signal, or for the request |ctx| or Index
		// Context to be cancelled.
		fi.mu.RUnlock()
		select {
		case <-condCh:
			// Pass.
		case <-ctx.Done():
			err = ctx.Err()
		case <-fi.ctx.Done():
			err = fi.ctx.Err()
		}
		fi.mu.RLock()

		if err != nil {
			return nil, nil, err
		}
	}
}

// Summary returns the [Begin, End) offset range of all Fragments in the index,
// and the persisted ModTime of the last Fragment (or zero, if it's local).
func (fi *Index) Summary() (int64, int64, int64) {
	defer fi.mu.RUnlock()
	fi.mu.RLock()

	if l := len(fi.set); l == 0 {
		return 0, 0, 0
	} else {
		return fi.set[0].Begin, fi.set[l-1].End, fi.set[l-1].ModTime
	}
}

// SpoolCommit adds local Spool Fragment |frag| to the index.
func (fi *Index) SpoolCommit(frag Fragment) {
	defer fi.mu.Unlock()
	fi.mu.Lock()

	fi.set, _ = fi.set.Add(frag)
	fi.local, _ = fi.local.Add(frag)
	fi.wakeBlockedQueries()
}

// ReplaceRemote replaces all remote Fragments in the index with |set|.
func (fi *Index) ReplaceRemote(set CoverSet) {
	defer fi.mu.Unlock()
	fi.mu.Lock()

	// Remove local fragments which are also present in |set|. This removes
	// references to held File instances, allowing them to be finalized by the
	// garbage collector. As Fragment Files have only the single open file-
	// descriptor and no remaining hard links, this also releases associated
	// disk and OS page buffer resources. Note that we cannot directly Close
	// these Fragment Files (and must instead rely on GC to collect them),
	// as they may still be referenced by concurrent read requests.
	fi.local = CoverSetDifference(fi.local, set)

	// Extend |set| with remaining local Fragments not already in |set|.
	for _, frag := range fi.local {
		var ok bool

		if set, ok = set.Add(frag); !ok {
			panic("expected local fragment to not be covered")
		}
	}

	fi.set = set
	fi.wakeBlockedQueries()

	select {
	case <-fi.firstRefreshCh:
		// Already closed.
	default:
		close(fi.firstRefreshCh)
	}
}

// wakeBlockedQueries wakes all queries waiting for an index update.
// fi.mu must already be held.
func (fi *Index) wakeBlockedQueries() {
	// Close |condCh| to signal to waiting readers that the index has updated.
	// Create a new condition channel for future readers to block on, while
	// awaiting the next index update.
	close(fi.condCh)
	fi.condCh = make(chan struct{})
}

// FirstRefreshCh returns a channel which signals if the Index
// as been refreshed with a remote store(s) listing at least once.
func (fi *Index) FirstRefreshCh() <-chan struct{} { return fi.firstRefreshCh }

// Inspect invokes the callback with a snapshot of all fragments in the Index.
// The callback must not modify the CoverSet, and during callback invocation
// no changes will be made to it.
// If an initial refresh of remote fragment store(s) hasn't yet been applied,
// Inspect will first block until it does (or context cancellation).
func (fi *Index) Inspect(ctx context.Context, callback func(CoverSet) error) error {
	select {
	case <-fi.firstRefreshCh:
		// Pass.
	case <-ctx.Done():
		return ctx.Err()
	case <-fi.ctx.Done():
		return fi.ctx.Err()
	}

	fi.mu.RLock()
	defer fi.mu.RUnlock()
	return callback(fi.set)
}

// WalkAllStores enumerates Fragments from each of |stores| into the returned
// CoverSet, or returns an encountered error.
func WalkAllStores(ctx context.Context, name pb.Journal, allStores []pb.FragmentStore) (CoverSet, error) {
	var set CoverSet

	for _, fs := range allStores {
		var store = stores.Get(fs)
		store.Mark.Store(true)

		var err = store.List(ctx, string(name)+"/", func(path string, modTime time.Time) error {
			// The path returned by List is already relative to the prefix
			if frag, err := pb.ParseFragmentFromRelativePath(name, path); err != nil {
				// It's not uncommon for extra files to live under a journal.
				// Don't fail the entire listing when this happens.
				logrus.WithFields(logrus.Fields{
					"path":    path,
					"journal": name,
					"modTime": modTime,
				}).Warn("failed to parse fragment")
			} else if frag.Journal != name {
				return fmt.Errorf("fragment %s is not of expected journal %s", &frag, name)
			} else {
				frag.ModTime = modTime.Unix()
				frag.BackingStore = fs
				set, _ = set.Add(Fragment{Fragment: frag})
			}
			return nil
		})
		if err != nil {
			return CoverSet{}, err
		}
	}
	return set, nil
}

var timeNow = time.Now

func addTrace(ctx context.Context, format string, args ...interface{}) {
	if tr, ok := trace.FromContext(ctx); ok {
		tr.LazyPrintf(format, args...)
	}
}
