package broker

import (
	"context"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	pbx "go.gazette.dev/core/broker/protocol/ext"
	"go.gazette.dev/core/broker/stores"
	"go.gazette.dev/core/keyspace"
)

// resolver maps journals to responsible broker instances and, potentially, a local replica.
type resolver struct {
	state *allocator.State
	// Set of local replicas known to this resolver. Nil iff
	// stopServingLocalReplicas() has been called.
	replicas map[pb.Journal]*resolverReplica
	// newReplica builds a new local replica instance.
	newReplica func(pb.Journal) *replica
	// wg synchronizes over all running local replicas.
	wg sync.WaitGroup
}

// resolverReplica extends a *replica instance with detection and signaling
// of changes to its assignments.
type resolverReplica struct {
	*replica                          // Embedded replica managing journal state and operations.
	assignments keyspace.KeyValues    // Current assignments of this journal from etcd.
	primary     bool                  // Whether this broker is the primary for this journal.
	signalCh    chan struct{}         // Closed when assignments change, to wake blocked RPCs.
	stores      []*stores.ActiveStore // Pre-fetched stores corresponding to the journal's FragmentStores.
}

func newResolver(state *allocator.State, newReplica func(pb.Journal) *replica) *resolver {
	var r = &resolver{
		state:      state,
		replicas:   make(map[pb.Journal]*resolverReplica),
		newReplica: newReplica,
	}
	state.KS.Mu.Lock()
	state.KS.Observers = append(state.KS.Observers, r.updateResolutions)
	state.KS.Mu.Unlock()
	return r
}

type resolveOpts struct {
	// Whether we may proxy to another broker.
	mayProxy bool
	// Whether we require the primary broker of the journal.
	requirePrimary bool
	// Minimum Etcd Revision to have read through, before generating a resolution.
	minEtcdRevision int64
	// Optional Header attached to the request from a proxy-ing peer.
	proxyHeader *pb.Header
}

type resolution struct {
	status pb.Status
	// ProcessSpec_ID of this broker.
	localID pb.ProcessSpec_ID
	// Header defines the effective Etcd Revision, Journal Route, and potentially
	// a specific broker ID of the resolution. A broker ID may be omitted if this
	// resolution is proxy-able to multiple peers, but is always specified if this
	// broker can locally serve the request, or the primary broker is required.
	pb.Header
	// Item (JournalSpec) of the Journal at the current Etcd Revision.
	item keyspace.KeyValue
	// Decoded JournalSpec extracted from `item`.
	journalSpec *pb.JournalSpec
	// Assignments of the Journal at the current Etcd Revision.
	assignments keyspace.KeyValues
	// Local replica of the assigned journal, if one exists.
	replica *replica
	// Pre-fetched ActiveStore instances corresponding to journalSpec.Fragment.Stores
	stores []*stores.ActiveStore
	// If |replica| is non-nil, |invalidateCh| is also, and is closed when
	// this resolution has been invalidated due to a subsequent assignment
	// update of the journal.
	invalidateCh <-chan struct{}
}

func (r *resolver) resolve(ctx context.Context, claims pb.Claims, journal pb.Journal, opts resolveOpts) (res *resolution, err error) {
	var ks = r.state.KS
	res = new(resolution)

	// Discard metadata path segment, which doesn't alter resolution outcomes.
	journal = journal.StripMeta()

	ks.Mu.RLock()
	defer ks.Mu.RUnlock()

	if r.state.LocalMemberInd != -1 {
		res.localID = r.state.Members[r.state.LocalMemberInd].
			Decoded.(allocator.Member).MemberValue.(*pb.BrokerSpec).Id
	} else {
		// During graceful shutdown, we may still serve requests even after our
		// local member key has been removed from Etcd. We don't want to outright
		// fail these requests as we can usefully proxy them. Use a placeholder
		// to ensure |localID| doesn't match ProcessSpec_ID{}, and for logging.
		res.localID = pb.ProcessSpec_ID{Zone: "local-BrokerSpec", Suffix: "missing-from-Etcd"}
	}

	if hdr := opts.proxyHeader; hdr != nil {
		// Verify the requestor is using our same Etcd cluster.
		if hdr.Etcd.ClusterId != ks.Header.ClusterId {
			err = fmt.Errorf("request Etcd ClusterId doesn't match our own (%d vs %d)",
				hdr.Etcd.ClusterId, ks.Header.ClusterId)
			return
		}
		// Verify the requestor reached the intended member.
		if hdr.ProcessId != (pb.ProcessSpec_ID{}) && hdr.ProcessId != res.localID {
			err = fmt.Errorf("request ProcessId doesn't match our own (%s vs %s)",
				&hdr.ProcessId, &res.localID)
			return
		}
		// We want to wait for the greater of a |proxyHeader| or |minEtcdRevision|.
		if opts.proxyHeader.Etcd.Revision > opts.minEtcdRevision {
			opts.minEtcdRevision = opts.proxyHeader.Etcd.Revision
		}
	}

	if opts.minEtcdRevision > ks.Header.Revision {
		addTrace(ctx, " ... at revision %d, but want at least %d",
			ks.Header.Revision, opts.minEtcdRevision)

		if err = ks.WaitForRevision(ctx, opts.minEtcdRevision); err != nil {
			return
		}
		addTrace(ctx, "WaitForRevision(%d) => %d",
			opts.minEtcdRevision, ks.Header.Revision)
	}
	res.Etcd = pbx.FromEtcdResponseHeader(ks.Header)

	// Extract Assignments.
	res.assignments = ks.KeyValues.Prefixed(
		allocator.ItemAssignmentsPrefix(ks, journal.String())).Copy()

	// Extract JournalSpec.
	if index, ok := ks.KeyValues.Search(allocator.ItemKey(ks, journal.String())); ok {
		var item = ks.KeyValues[index]
		var spec = item.Decoded.(allocator.Item).ItemValue.(*pb.JournalSpec)

		// Is the caller authorized to the journal?
		if claims.Selector.Matches(spec.LabelSetExt(pb.LabelSet{
			Labels: make([]pb.Label, 0, 1+len(spec.LabelSet.Labels)),
		})) {
			res.item = item
			res.journalSpec = spec
		} else {
			// Clear to act as if the journal doesn't exist.
			res.assignments = keyspace.KeyValues{}
		}
	}

	// Build Route from extracted assignments.
	pbx.Init(&res.Route, res.assignments)
	pbx.AttachEndpoints(&res.Route, ks)

	// Select a definite ProcessID if we require the primary and there is one,
	// or if we're a member of the Route (and authoritative).
	if opts.requirePrimary && res.Route.Primary != -1 {
		res.ProcessId = res.Route.Members[res.Route.Primary]
	} else if !opts.requirePrimary {
		for i := range res.Route.Members {
			if res.Route.Members[i] == res.localID {
				res.ProcessId = res.localID
			}
		}
	}

	// If the journal is assigned locally, attach our replica to the resolution.
	if r.replicas == nil && res.ProcessId == res.localID {
		// The journal still resolves to this broker, but we've stopped local
		// replicas. This happens if the broker is shutting down abnormally
		// (eg, due to Etcd lease keep-alive failure) but its local KeySpace
		// doesn't reflect the re-assignment of the journal. Return an error
		// to immediately fail related RPCs, as we are probably trying to drain
		// the gRPC server and we cannot hope to successfully proxy RPCs if we
		// don't have updated route assignments.
		err = errResolverStopped
		return
	} else if replica := r.replicas[journal]; replica != nil {
		res.replica = replica.replica
		res.invalidateCh = replica.signalCh
		res.stores = replica.stores
	}

	// Select a response Status code.
	if res.journalSpec == nil {
		res.status = pb.Status_JOURNAL_NOT_FOUND
	} else if res.journalSpec.Suspend.GetLevel() == pb.JournalSpec_Suspend_FULL {
		res.status = pb.Status_SUSPENDED
	} else if opts.requirePrimary && res.Route.Primary == -1 {
		res.status = pb.Status_NO_JOURNAL_PRIMARY_BROKER
	} else if len(res.Route.Members) == 0 {
		res.status = pb.Status_INSUFFICIENT_JOURNAL_BROKERS
	} else if !opts.mayProxy && res.ProcessId != res.localID {
		if opts.requirePrimary {
			res.status = pb.Status_NOT_JOURNAL_PRIMARY_BROKER
		} else {
			res.status = pb.Status_NOT_JOURNAL_BROKER
		}
	} else {
		res.status = pb.Status_OK
	}

	// If we're returning an error, the effective ProcessId is ourselves
	// (since we authored the error response).
	if res.status != pb.Status_OK {
		res.ProcessId = res.localID
	}

	addTrace(ctx, "resolve(%s) => %s, local: %t, header: %s",
		journal, res.status, res.replica != nil, &res.Header)

	return
}

// updateResolutions, by virtue of being a KeySpace.Observer, expects that the
// KeySpace.Mu Lock is held.
func (r *resolver) updateResolutions() {
	if r.replicas == nil {
		return // We've stopped serving local replicas.
	}
	var next = make(map[pb.Journal]*resolverReplica, len(r.state.LocalItems))

	for _, li := range r.state.LocalItems {
		var (
			item        = li.Item.Decoded.(allocator.Item)
			spec        = item.ItemValue.(*pb.JournalSpec)
			name        = pb.Journal(item.ID)
			primary     = li.Assignments[li.Index].Decoded.(allocator.Assignment).Slot == 0
			replica, ok = r.replicas[name]
		)

		if _, ok := next[name]; ok {
			// TODO(johnny): This SHOULD not happen, but sometimes does.
			// If it does, we don't want to create extra replicas that are unlinked.
			continue
		}

		// If the replica is not found, or if it's `primary` but we have been demoted,
		// then create a new replica and tear down the old (if there is one).
		if !ok || !primary && replica.primary {
			r.wg.Add(1)
			replica = &resolverReplica{
				replica:     r.newReplica(name), // Newly assigned journal.
				primary:     primary,
				assignments: li.Assignments.Copy(),
				signalCh:    make(chan struct{}),
			}

			var rt pb.Route
			pbx.Init(&rt, li.Assignments)

			log.WithFields(log.Fields{
				"name":    replica.journal,
				"primary": primary,
			}).Info("starting local journal replica")

		} else {
			replica.primary = primary
			delete(r.replicas, name)
		}
		next[name] = replica

		// Update `stores` if the JournalSpec's FragmentStores have changed.
		var storesChanged = len(replica.stores) != len(spec.Fragment.Stores)
		if !storesChanged {
			for i, fs := range spec.Fragment.Stores {
				if replica.stores[i].Key != fs {
					storesChanged = true
					break
				}
			}
		}
		if storesChanged {
			var newStores = make([]*stores.ActiveStore, len(spec.Fragment.Stores))
			for i, fs := range spec.Fragment.Stores {
				newStores[i] = stores.Get(fs)
			}
			replica.stores = newStores
		}

		if !li.Assignments.EqualKeyRevisions(replica.assignments) || storesChanged {
			close(replica.signalCh)
			replica.signalCh = make(chan struct{})
			replica.assignments = li.Assignments.Copy()
		}
	}

	var prev = r.replicas
	r.replicas = next

	// Any remaining replicas in |prev| were not in LocalItems.
	r.cancelReplicas(prev)
}

// stopServingLocalReplicas begins immediate shutdown of any & all local
// replicas, and causes future attempts to resolve to local replicas to
// return an error.
func (r *resolver) stopServingLocalReplicas() {
	r.state.KS.Mu.Lock()
	defer r.state.KS.Mu.Unlock()

	r.cancelReplicas(r.replicas)
	r.replicas = nil
}

func (r *resolver) cancelReplicas(m map[pb.Journal]*resolverReplica) {
	for _, replica := range m {
		log.WithFields(log.Fields{
			"name":    replica.journal,
			"primary": replica.primary,
		}).Info("stopping local journal replica")

		// Close |signalCh| to unblock any Replicate or Append RPCs which would
		// otherwise race shutDownReplica() to the |spoolCh| or |pipelineCh|.
		close(replica.signalCh)

		go shutDownReplica(replica.replica, r.wg.Done)
	}
}

func (r *resolver) watch(ctx context.Context, etcd *clientv3.Client) error {
	var err = r.state.KS.Watch(ctx, etcd)
	if errors.Cause(err) == context.Canceled {
		err = nil
	}
	return err
}

var errResolverStopped = errors.New("resolver has stopped serving local replicas")
