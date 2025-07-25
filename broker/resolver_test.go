package broker

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	pbx "go.gazette.dev/core/broker/protocol/ext"
	"go.gazette.dev/core/broker/stores"
	"go.gazette.dev/core/etcdtest"
)

func TestResolveCases(t *testing.T) {
	var ctx, etcd = context.Background(), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var peer = newMockBroker(t, etcd, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	var mkRoute = func(i int, ids ...pb.ProcessSpec_ID) (rt pb.Route) {
		rt = pb.Route{Primary: int32(i)}
		for _, id := range ids {
			var ep pb.Endpoint

			switch id {
			case broker.id:
				ep = broker.srv.Endpoint()
			case peer.id:
				ep = peer.Endpoint()
			default:
				panic("bad id")
			}
			rt.Members = append(rt.Members, id)
			rt.Endpoints = append(rt.Endpoints, ep)
		}
		return
	}
	setTestJournal(broker, pb.JournalSpec{Name: "primary/journal", Replication: 2},
		broker.id, peer.id)
	setTestJournal(broker, pb.JournalSpec{Name: "replica/journal", Replication: 2},
		peer.id, broker.id)
	setTestJournal(broker, pb.JournalSpec{Name: "no/primary/journal", Replication: 2},
		pb.ProcessSpec_ID{}, broker.id, peer.id)
	setTestJournal(broker, pb.JournalSpec{Name: "no/brokers/journal", Replication: 2})
	setTestJournal(broker, pb.JournalSpec{Name: "peer/only/journal", Replication: 1},
		peer.id)

	var resolver = broker.svc.resolver // Less typing.

	// Expect a replica was created for each journal |broker| is responsible for.
	require.Len(t, resolver.replicas, 3)

	// Case: simple resolution of local replica.
	var r, _ = resolver.resolve(ctx, allClaims, "replica/journal;ignored/meta", resolveOpts{})
	require.Equal(t, pb.Status_OK, r.status)
	// Expect the local replica is attached.
	require.Equal(t, resolver.replicas["replica/journal"].replica, r.replica)
	require.NotNil(t, r.invalidateCh)
	// As is the JournalSpec
	require.Equal(t, pb.Journal("replica/journal"), r.journalSpec.Name)
	// And a Header having the correct Route (with Endpoints), Etcd header, and responsible broker ID.
	require.Equal(t, mkRoute(1, broker.id, peer.id), r.Header.Route)
	require.Equal(t, pbx.FromEtcdResponseHeader(broker.ks.Header), r.Header.Etcd)
	// The ProcessID was set to this broker, as it resolves to a local replica.
	require.Equal(t, broker.id, r.Header.ProcessId)
	// And the localID was populated.
	require.Equal(t, broker.id, r.localID)

	// Case: primary is required, and we are primary.
	r, _ = resolver.resolve(ctx, allClaims, "primary/journal", resolveOpts{requirePrimary: true})
	require.Equal(t, pb.Status_OK, r.status)
	require.Equal(t, broker.id, r.Header.ProcessId)
	require.Equal(t, mkRoute(0, broker.id, peer.id), r.Header.Route)

	// Case: primary is required, we are not primary, and may not proxy.
	r, _ = resolver.resolve(ctx, allClaims, "replica/journal", resolveOpts{requirePrimary: true})
	require.Equal(t, pb.Status_NOT_JOURNAL_PRIMARY_BROKER, r.status)
	// As status != OK and we authored the resolution, ProcessId is still |broker|.
	require.Equal(t, broker.id, r.Header.ProcessId)
	// The current route is attached, allowing the client to resolve the discrepancy.
	require.Equal(t, mkRoute(1, broker.id, peer.id), r.Header.Route)
	// As we have a replica, it's attached.
	require.NotNil(t, r.replica)

	// Case: primary is required, and we may proxy.
	r, _ = resolver.resolve(ctx, allClaims, "replica/journal", resolveOpts{requirePrimary: true, mayProxy: true})
	require.Equal(t, pb.Status_OK, r.status)
	// The resolution is specifically to |peer|.
	require.Equal(t, peer.id, r.Header.ProcessId)
	require.Equal(t, mkRoute(1, broker.id, peer.id), r.Header.Route)
	// Replica is also attached.
	require.NotNil(t, r.replica)

	// Case: primary is required, we may proxy, but there is no primary.
	r, _ = resolver.resolve(ctx, allClaims, "no/primary/journal", resolveOpts{requirePrimary: true, mayProxy: true})
	require.Equal(t, pb.Status_NO_JOURNAL_PRIMARY_BROKER, r.status)
	require.Equal(t, broker.id, r.Header.ProcessId) // We authored the error.
	require.Equal(t, mkRoute(-1, broker.id, peer.id), r.Header.Route)
	require.NotNil(t, r.replica)

	// Case: we may not proxy, and are not a replica.
	r, _ = resolver.resolve(ctx, allClaims, "peer/only/journal", resolveOpts{})
	require.Equal(t, pb.Status_NOT_JOURNAL_BROKER, r.status)
	require.Equal(t, broker.id, r.Header.ProcessId) // We authored the error.
	require.Equal(t, mkRoute(0, peer.id), r.Header.Route)
	require.Nil(t, r.replica)

	// Case: we may proxy, and are not a replica.
	r, _ = resolver.resolve(ctx, allClaims, "peer/only/journal", resolveOpts{mayProxy: true})
	require.Equal(t, pb.Status_OK, r.status)
	// ProcessId is left empty as we could proxy to any of multiple peers.
	require.Equal(t, pb.ProcessSpec_ID{}, r.Header.ProcessId)
	require.Equal(t, mkRoute(0, peer.id), r.Header.Route)
	require.Nil(t, r.replica)

	// Case: the journal has no brokers.
	r, _ = resolver.resolve(ctx, allClaims, "no/brokers/journal", resolveOpts{mayProxy: true})
	require.Equal(t, pb.Status_INSUFFICIENT_JOURNAL_BROKERS, r.status)
	require.Equal(t, broker.id, r.Header.ProcessId)
	require.Equal(t, mkRoute(-1), r.Header.Route)

	// Case: the journal doesn't exist.
	r, _ = resolver.resolve(ctx, allClaims, "does/not/exist", resolveOpts{})
	require.Equal(t, pb.Status_JOURNAL_NOT_FOUND, r.status)
	require.Equal(t, broker.id, r.Header.ProcessId)
	require.Equal(t, mkRoute(-1), r.Header.Route)

	// Case: our broker key has been removed.
	var resp, err = etcd.Delete(ctx, resolver.state.LocalKey)
	require.NoError(t, err)

	broker.ks.Mu.RLock()
	require.NoError(t, broker.ks.WaitForRevision(ctx, resp.Header.Revision))
	broker.ks.Mu.RUnlock()

	// Subcase 1: We can still resolve for peer journals.
	r, _ = resolver.resolve(ctx, allClaims, "peer/only/journal", resolveOpts{mayProxy: true})
	require.Equal(t, pb.Status_OK, r.status)
	require.Equal(t, pb.ProcessSpec_ID{}, r.Header.ProcessId)
	require.Equal(t, mkRoute(0, peer.id), r.Header.Route)
	require.Nil(t, r.replica)

	// Subcase 2: We use a placeholder ProcessId.
	r, _ = resolver.resolve(ctx, allClaims, "peer/only/journal", resolveOpts{})
	require.Equal(t, pb.Status_NOT_JOURNAL_BROKER, r.status)
	require.Equal(t, pb.ProcessSpec_ID{Zone: "local-BrokerSpec", Suffix: "missing-from-Etcd"}, r.Header.ProcessId)
	require.Equal(t, mkRoute(0, peer.id), r.Header.Route)

	broker.cleanup()
}

func TestResolverLocalReplicaStopping(t *testing.T) {
	var ctx, etcd = context.Background(), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var peer = newMockBroker(t, etcd, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1}, broker.id)

	// Precondition: journal & replica resolve as per expectation.
	var r, _ = broker.svc.resolver.resolve(ctx, allClaims, "a/journal", resolveOpts{})
	require.Equal(t, pb.Status_OK, r.status)
	require.Equal(t, broker.id, r.Header.ProcessId)
	require.NotNil(t, r.replica)
	require.NoError(t, r.replica.ctx.Err())

	// Initially, we're primary for `peer/journal`.
	setTestJournal(broker, pb.JournalSpec{Name: "peer/journal", Replication: 1}, broker.id)

	r, _ = broker.svc.resolver.resolve(ctx, allClaims, "peer/journal", resolveOpts{})
	require.Equal(t, pb.Status_OK, r.status)
	require.Equal(t, broker.id, r.Header.ProcessId)
	require.NotNil(t, r.replica)

	var prevReplica = r.replica

	// We're demoted to replica for `peer/journal`.
	setTestJournal(broker, pb.JournalSpec{Name: "peer/journal", Replication: 1}, peer.id, broker.id)

	r, _ = broker.svc.resolver.resolve(ctx, allClaims, "peer/journal", resolveOpts{})
	require.Equal(t, pb.Status_OK, r.status)
	require.Equal(t, broker.id, r.Header.ProcessId)
	require.NotNil(t, r.replica)
	require.False(t, prevReplica == r.replica) // Expect a new replica was created.

	// Peer is wholly responsible for `peer/journal`.
	setTestJournal(broker, pb.JournalSpec{Name: "peer/journal", Replication: 1}, peer.id)

	broker.svc.resolver.stopServingLocalReplicas()

	// Expect a route invalidation occurred immediately, to wake any awaiting RPCs.
	<-r.invalidateCh
	// And that the replica is then shut down.
	<-r.replica.ctx.Done()

	// Attempts to resolve a local journal fail.
	var _, err = broker.svc.resolver.resolve(ctx, allClaims, "a/journal", resolveOpts{})
	require.Equal(t, errResolverStopped, err)
	// However we'll still return proxy resolutions to peers.
	r, _ = broker.svc.resolver.resolve(ctx, allClaims, "peer/journal", resolveOpts{requirePrimary: true, mayProxy: true})
	require.Equal(t, pb.Status_OK, r.status)
	require.Equal(t, peer.id, r.Header.ProcessId)

	// Assign new local & peer journals.
	setTestJournal(broker, pb.JournalSpec{Name: "new/local/journal", Replication: 1}, broker.id)
	setTestJournal(broker, pb.JournalSpec{Name: "new/peer/journal", Replication: 1}, peer.id)

	// An attempt for this new local journal still fails.
	_, err = broker.svc.resolver.resolve(ctx, allClaims, "a/journal", resolveOpts{})
	require.Equal(t, errResolverStopped, err)
	// But we successfully resolve to a peer.
	r, _ = broker.svc.resolver.resolve(ctx, allClaims, "peer/journal", resolveOpts{requirePrimary: true, mayProxy: true})
	require.Equal(t, pb.Status_OK, r.status)
	require.Equal(t, peer.id, r.Header.ProcessId)

	broker.cleanup()
	peer.Cleanup()
}

func TestResolveFutureRevisionCasesWithProxyHeader(t *testing.T) {
	var ctx, etcd = context.Background(), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})

	// Case: Request a resolution, passing along a proxyHeader fixture which
	// references a future Etcd Revision. In the background, arrange for that Etcd
	// update to be delivered.

	var hdr = pb.Header{
		ProcessId: broker.id,
		Route: pb.Route{
			Members:   []pb.ProcessSpec_ID{broker.id},
			Primary:   0,
			Endpoints: []pb.Endpoint{broker.srv.Endpoint()},
		},
		Etcd: pbx.FromEtcdResponseHeader(broker.ks.Header),
	}
	hdr.Etcd.Revision += 1

	time.AfterFunc(time.Millisecond, func() {
		setTestJournal(broker, pb.JournalSpec{Name: "journal/one", Replication: 1}, broker.id)
	})

	// Expect the resolution succeeds, despite the journal not yet existing.
	var r, _ = broker.svc.resolver.resolve(ctx, allClaims, "journal/one", resolveOpts{proxyHeader: &hdr})
	require.Equal(t, pb.Status_OK, r.status)
	require.Equal(t, hdr, r.Header)

	// Case: this time, specify a future revision via |minEtcdRevision|. Expect that also works.
	var futureRevision = broker.ks.Header.Revision + 1
	// Race an update of the journal with resolve(futureRevision).
	time.AfterFunc(time.Millisecond, func() {
		setTestJournal(broker, pb.JournalSpec{Name: "journal/two", Replication: 1}, broker.id)
	})
	r, _ = broker.svc.resolver.resolve(ctx, allClaims, "journal/two", resolveOpts{minEtcdRevision: futureRevision})
	require.Equal(t, pb.Status_OK, r.status)

	// Case: finally, specify a future revision which doesn't come about and cancel the context.
	ctx, cancel := context.WithCancel(ctx)
	time.AfterFunc(time.Millisecond, cancel)

	var _, err = broker.svc.resolver.resolve(ctx, allClaims, "journal/three", resolveOpts{minEtcdRevision: futureRevision + 1e10})
	require.Equal(t, context.Canceled, err)

	broker.cleanup()
}

func TestResolveProxyHeaderErrorCases(t *testing.T) {
	var ctx, etcd = context.Background(), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})

	var proxy = pb.Header{
		ProcessId: pb.ProcessSpec_ID{Zone: "other", Suffix: "id"},
		Route:     pb.Route{Primary: -1},
		Etcd:      pbx.FromEtcdResponseHeader(broker.ks.Header),
	}

	// Case: proxy header references a broker other than this one.
	var _, err = broker.svc.resolver.resolve(ctx, allClaims, "a/journal", resolveOpts{proxyHeader: &proxy})
	require.Regexp(t, `request ProcessId doesn't match our own \(zone.*`, err)
	proxy.ProcessId = broker.id

	// Case: proxy header references a ClusterId other than our own.
	proxy.Etcd.ClusterId = 8675309
	_, err = broker.svc.resolver.resolve(ctx, allClaims, "a/journal", resolveOpts{proxyHeader: &proxy})
	require.Regexp(t, `request Etcd ClusterId doesn't match our own \(\d+.*`, err)

	broker.cleanup()
}

func TestResolverStoresHandling(t *testing.T) {
	var ctx, etcd = context.Background(), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	stores.RegisterProviders(map[string]stores.Constructor{
		"s3": func(ep *url.URL) (stores.Store, error) {
			return stores.NewMemoryStore(ep), nil
		},
	})

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var peer = newMockBroker(t, etcd, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	// Case 1: Create journal with fragment stores
	var spec = pb.JournalSpec{
		Name:        "stores/journal",
		Replication: 2,
		Fragment: pb.JournalSpec_Fragment{
			Length:          1024,
			RefreshInterval: time.Second,
			Stores:          []pb.FragmentStore{"s3://store1/", "s3://store2/"},
		},
	}
	setTestJournal(broker, spec, broker.id, peer.id)

	// Verify replica has stores initialized
	var replica = broker.svc.resolver.replicas["stores/journal"]
	require.NotNil(t, replica)
	require.Len(t, replica.stores, 2)
	require.Equal(t, pb.FragmentStore("s3://store1/"), replica.stores[0].Key)
	require.Equal(t, pb.FragmentStore("s3://store2/"), replica.stores[1].Key)

	// Case 2: Resolve journal and verify stores are copied to resolution
	var r, _ = broker.svc.resolver.resolve(ctx, allClaims, "stores/journal", resolveOpts{})
	require.Equal(t, pb.Status_OK, r.status)
	require.Len(t, r.stores, 2)
	require.Equal(t, pb.FragmentStore("s3://store1/"), r.stores[0].Key)
	require.Equal(t, pb.FragmentStore("s3://store2/"), r.stores[1].Key)

	// Case 3: Update journal to change fragment stores
	// First capture the current signalCh to verify it gets closed
	var origSignalCh = replica.signalCh

	spec.Fragment.Stores = []pb.FragmentStore{"s3://store3/"}
	setTestJournal(broker, spec, broker.id, peer.id)

	// Verify stores were updated
	replica = broker.svc.resolver.replicas["stores/journal"]
	require.NotNil(t, replica)
	require.Len(t, replica.stores, 1)
	require.Equal(t, pb.FragmentStore("s3://store3/"), replica.stores[0].Key)

	// Verify signalCh was closed when stores changed
	select {
	case <-origSignalCh:
		// Good - channel was closed
	case <-time.After(100 * time.Millisecond):
		require.Fail(t, "signalCh was not closed when stores changed")
	}

	// Case 4: Update journal to add more stores
	spec.Fragment.Stores = []pb.FragmentStore{"s3://store3/", "s3://store4/", "s3://store5/"}
	setTestJournal(broker, spec, broker.id, peer.id)

	replica = broker.svc.resolver.replicas["stores/journal"]
	require.NotNil(t, replica)
	require.Len(t, replica.stores, 3)
	require.Equal(t, pb.FragmentStore("s3://store3/"), replica.stores[0].Key)
	require.Equal(t, pb.FragmentStore("s3://store4/"), replica.stores[1].Key)
	require.Equal(t, pb.FragmentStore("s3://store5/"), replica.stores[2].Key)

	// Case 5: Update journal to clear stores
	spec.Fragment.Stores = nil
	setTestJournal(broker, spec, broker.id, peer.id)

	replica = broker.svc.resolver.replicas["stores/journal"]
	require.NotNil(t, replica)
	require.Len(t, replica.stores, 0)

	// Case 6: Create journal with no stores
	var spec2 = pb.JournalSpec{
		Name:        "no-stores/journal",
		Replication: 2,
	}
	setTestJournal(broker, spec2, broker.id, peer.id)

	replica = broker.svc.resolver.replicas["no-stores/journal"]
	require.NotNil(t, replica)
	require.Len(t, replica.stores, 0)

	// Case 7: Resolve journal with no stores
	r, _ = broker.svc.resolver.resolve(ctx, allClaims, "no-stores/journal", resolveOpts{})
	require.Equal(t, pb.Status_OK, r.status)
	require.Len(t, r.stores, 0)

	// Case 8: Verify stores are not updated when they haven't changed
	spec.Fragment.Stores = []pb.FragmentStore{"s3://final/"}
	setTestJournal(broker, spec, broker.id, peer.id)

	replica = broker.svc.resolver.replicas["stores/journal"]
	var storesSlice1 = replica.stores
	var signalCh1 = replica.signalCh

	// Update with same stores - should reuse same slice and NOT close signalCh
	setTestJournal(broker, spec, broker.id, peer.id)

	replica = broker.svc.resolver.replicas["stores/journal"]
	var storesSlice2 = replica.stores

	// Verify same slice is reused (pointer equality)
	require.True(t, &storesSlice1[0] == &storesSlice2[0])

	// Verify signalCh was NOT closed (it's the same channel)
	require.Equal(t, signalCh1, replica.signalCh)
	select {
	case <-signalCh1:
		require.Fail(t, "signalCh was closed when stores didn't change")
	default:
		// Good - channel is still open
	}

	broker.cleanup()
	peer.Cleanup()
}
