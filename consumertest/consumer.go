// Package consumertest provides utilities for in-process unit testing of Gazette consumer applications.
package consumertest

import (
	"context"
	"os"
	"syscall"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/auth"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	pbx "go.gazette.dev/core/broker/protocol/ext"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
)

// Consumer is a lightweight, embedded Gazette consumer runtime suitable for
// in-process testing of consumer applications.
type Consumer struct {
	// Server is a loopback Server created for this Consumer, which is available
	// for test applications to register APIs against.
	Server *server.Server
	// Service of the Consumer, which is available for test applications.
	Service *consumer.Service
	// Tasks of the Consumer.
	Tasks *task.Group

	sigCh chan<- os.Signal
}

// Args of NewConsumer.
type Args struct {
	C        require.TestingT
	Etcd     *clientv3.Client       // Etcd client instance.
	Journals pb.RoutedJournalClient // Broker client instance.
	App      consumer.Application   // Application of the consumer.
	Root     string                 // Consumer root in Etcd. Defaults to "/consumertest".
	Zone     string                 // Zone of the consumer. Defaults to "local".
	Suffix   string                 // ID Suffix of the consumer. Defaults to "consumer".
	AuthKeys string                 // Authentication keys. Defaults to base64("secret") ("c2VjcmV0").
}

// NewConsumer builds and returns a Consumer.
func NewConsumer(args Args) *Consumer {
	if args.Root == "" {
		args.Root = "/consumer.test"
	}
	if args.Zone == "" {
		args.Zone = "local"
	}
	if args.Suffix == "" {
		args.Suffix = "consumer"
	}
	if args.AuthKeys == "" {
		args.AuthKeys = "c2VjcmV0"
	}
	var auth, err = auth.NewKeyedAuth(args.AuthKeys)
	require.NoError(args.C, err)

	var (
		id        = pb.ProcessSpec_ID{Zone: args.Zone, Suffix: args.Suffix}
		ks        = consumer.NewKeySpace(args.Root)
		state     = allocator.NewObservedState(ks, allocator.MemberKey(ks, id.Zone, id.Suffix), consumer.ShardIsConsistent)
		srv       = server.MustLoopback()
		svc       = consumer.NewService(args.App, auth, auth, state, args.Journals, srv.GRPCLoopback, args.Etcd)
		tasks     = task.NewGroup(context.Background())
		sigCh     = make(chan os.Signal, 1)
		allocArgs = allocator.SessionArgs{
			Etcd:     args.Etcd,
			Tasks:    tasks,
			LeaseTTL: time.Second * 60,
			SignalCh: sigCh,
			Spec: &pc.ConsumerSpec{
				ProcessSpec: pb.ProcessSpec{Id: id, Endpoint: srv.Endpoint()},
				ShardLimit:  100,
			},
			State: state,
		}
	)

	require.NoError(args.C, allocator.StartSession(allocArgs))
	pc.RegisterShardServer(srv.GRPCServer, pc.NewVerifiedShardServer(svc, auth))
	ks.WatchApplyDelay = 0 // Speedup test execution.

	srv.QueueTasks(tasks)
	svc.QueueTasks(tasks, srv)

	return &Consumer{
		Service: svc,
		Server:  srv,
		Tasks:   tasks,
		sigCh:   sigCh,
	}
}

// Signal the Consumer. The test Consumer will eventually exit,
// assuming other Consumers(s) are available to take over the assignments.
func (cmr *Consumer) Signal() { cmr.sigCh <- syscall.SIGTERM }

// WaitForPrimary of the identified shard until the Context is cancelled.
// If no error occurs, then the shard has a primary *Consumer (which is not
// necessarily this *Consumer instance). If |routeOut| is non-nil, it's populated
// with the current shard Route.
func (cmr *Consumer) WaitForPrimary(ctx context.Context, shard pc.ShardID, routeOut *pb.Route) error {
	var resp, err = cmr.Service.Etcd.Get(ctx, "a-key-that-doesn't-exist")
	if err != nil {
		return err
	}

	var rev = resp.Header.Revision
	var ks = cmr.Service.State.KS

	ks.Mu.RLock()
	defer ks.Mu.RUnlock()

	for {
		if err = ks.WaitForRevision(ctx, rev); err != nil {
			return err
		}
		// Walk assignments and determine if the slot-zero one is PRIMARY.
		var asn = ks.KeyValues.Prefixed(allocator.ItemAssignmentsPrefix(ks, shard.String()))
		for _, a := range asn {
			var (
				decoded = a.Decoded.(allocator.Assignment)
				status  = decoded.AssignmentValue.(*pc.ReplicaStatus)
			)
			if decoded.Slot == 0 && status.Code == pc.ReplicaStatus_PRIMARY {
				if routeOut != nil {
					pbx.Init(routeOut, asn)
				}
				return nil // Success.
			}
		}
		// Block for the next KeySpace update.
		rev = ks.Header.Revision + 1
	}
}

// CreateShards using the Consumer Apply API, and wait for them to be allocated.
func CreateShards(t require.TestingT, cmr *Consumer, specs ...*pc.ShardSpec) {
	var req = new(pc.ApplyRequest)
	for _, spec := range specs {
		req.Changes = append(req.Changes, pc.ApplyRequest_Change{Upsert: spec})
	}

	var sc = pc.NewShardClient(cmr.Service.Loopback)
	sc = pc.NewAuthShardClient(sc, cmr.Service.Authorizer)

	var resp, err = consumer.ApplyShards(context.Background(), sc, req)
	require.NoError(t, err)
	require.Equal(t, pc.Status_OK, resp.Status)

	for _, s := range specs {
		require.NoError(t, cmr.WaitForPrimary(context.Background(), s.Id, nil))
	}
}

// WaitForShards queries for shards matching LabelSelector |sel|, determines
// the current write-heads of journals being consumed by matched shards, and
// polls shards until each has caught up to the determined write-heads of its
// consumed journals.
func WaitForShards(t require.TestingT, cmr *Consumer, sel pb.LabelSelector) {
	var sc = pc.NewShardClient(cmr.Service.Loopback)
	sc = pc.NewAuthShardClient(sc, cmr.Service.Authorizer)

	var ctx = pb.WithDispatchDefault(context.Background())

	var shards, err = consumer.ListShards(ctx, sc, &pc.ListRequest{Selector: sel})
	require.NoError(t, err)

	// Collect the set of journals being read by shards.
	var expect = make(map[pb.Journal]int64)
	for _, shard := range shards.Shards {
		for _, src := range shard.Spec.Sources {
			expect[src.Journal] = 0
		}
	}
	// Determine the write-head of each journal.
	for journal := range expect {
		var r = client.NewReader(ctx, cmr.Service.Journals, pb.ReadRequest{
			Journal: journal,
			Offset:  -1,
			Block:   false,
		})
		if _, err = r.Read(nil); err != client.ErrOffsetNotYetAvailable {
			require.NoError(t, err)
		}
		expect[journal] = r.Response.WriteHead
	}
	// Stat each shard, blocking until it reads through journal write-heads.
	for len(shards.Shards) != 0 {
		_, err = sc.Stat(ctx, &pc.StatRequest{
			Shard:       shards.Shards[0].Spec.Id,
			ReadThrough: expect,
		})
		require.NoError(t, err)
		shards.Shards = shards.Shards[1:]
	}
}
