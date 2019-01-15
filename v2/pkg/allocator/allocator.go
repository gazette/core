package allocator

import (
	"context"
	"fmt"
	"strings"

	"github.com/LiveRamp/gazette/v2/pkg/allocator/push_relabel"
	"github.com/LiveRamp/gazette/v2/pkg/keyspace"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

type AllocateArgs struct {
	Context context.Context
	// Etcd client Allocate will use to effect changes to the distributed allocation.
	Etcd *clientv3.Client
	// Allocator state, which is derived from a Watched KeySpace.
	State *State
	// TestHook is an optional testing hook, invoked after each convergence round.
	TestHook func(round int, isIdle bool)
}

// Allocate observes the Allocator KeySpace, and if this Allocator instance is
// the current leader, performs reactive scheduling rounds to maintain the
// allocation of all Items to Members. Allocate exits on an unrecoverable
// error, or if:
//
//   * The local Member has an ItemLimit of Zero, AND
//   * No Assignments to the current Member remain.
//
// Eg, Allocate should be gracefully stopped by updating the ItemLimit of the
// Member identified by Allocator.LocalKey() to zero (perhaps as part of a
// SIGTERM signal handler) and then waiting for Allocate to return, which it
// will once all instance Assignments have been re-assigned to other Members.
func Allocate(args AllocateArgs) error {
	// flowNetwork is local to a single pass of the scheduler, but we retain a
	// single instance and re-use it each iteration to reduce allocation.
	var fn = new(flowNetwork)
	// The leader runs push/relabel to re-compute a |desired| network only when
	// the State |NetworkHash| changes. Otherwise, it incrementally converges
	// towards the previous solution, which is still a valid maximum assignment.
	// This caching is both more efficient, and also mitigates the impact of
	// small instabilities in the prioritized push/relabel solution.
	var desired []Assignment
	var lastNetworkHash uint64

	var state = args.State
	var ks = args.State.KS
	var ctx = args.Context
	var round int

	defer ks.Mu.RUnlock()
	ks.Mu.RLock()

	for {
		if state.LocalMemberInd == -1 {
			return fmt.Errorf("member key not found in Etcd")
		} else if state.shouldExit() {
			return nil
		}

		// Response of the last transaction we applied. We'll ensure we've minimally
		// watched through its revision before driving further action.
		var txnResponse *clientv3.TxnResponse

		if state.isLeader() {

			// Do we need to re-solve for a maximum assignment?
			if state.NetworkHash != lastNetworkHash {
				lastNetworkHash = state.NetworkHash

				// Build a prioritized flowNetwork and solve for maximum flow.
				fn.init(state)
				push_relabel.FindMaxFlow(&fn.source, &fn.sink)

				// Extract desired max-flow Assignments for each Item.
				desired = desired[:0]
				for item := range state.Items {
					desired = extractItemFlow(state, fn, item, desired)
				}

				if len(desired) < state.ItemSlots {
					// We cannot assign each Item to the desired number of replicas. Most likely,
					// there are too few Members or they are poorly distributed across zones.
					log.WithField("unattainable_replicas", state.ItemSlots-len(desired)).
						Warn("cannot reach desired replication for all items")
				}
			}

			// Use batched transactions to amortize the network cost of Etcd updates,
			// and re-verify our Member key with each flush to ensure we're still leader.
			var txn = newBatchedTxn(ctx, args.Etcd,
				modRevisionUnchanged(state.Members[state.LocalMemberInd]))

			// Converge the current state towards |desired|.
			var err error
			if err = converge(txn, state, desired); err == nil {
				txnResponse, err = txn.Commit()
			}

			if err != nil {
				log.WithFields(log.Fields{"err": err, "round": round, "rev": ks.Header.Revision}).
					Warn("converge iteration failed (will retry)")
			} else {
				if args.TestHook != nil {
					args.TestHook(round, txn.noop)
				}
				round++
			}
		}

		// Await the next KeySpace change. If we completed a transaction,
		// ensure we read through its revision before iterating again.
		var next = ks.Header.Revision + 1

		if txnResponse != nil && txnResponse.Header.Revision > next {
			next = txnResponse.Header.Revision
		}
		if err := ks.WaitForRevision(ctx, next); err != nil {
			return err
		}
	}
}

// converge identifies and applies allowed incremental changes which bring the
// current state closer to the |desired| state. A change is allowed iff it does
// not cause any Item or Member replication constraints to be violated (eg, by
// leaving an Item with too few consistent replicas, or a Member with too many
// assigned Items).
func converge(txn checkpointTxn, as *State, desired []Assignment) error {
	var itemState = itemState{global: as}
	var lastCRE int // cur.RightEnd of the previous iteration.

	// Walk Items, joined with their current Assignments. Simultaneously walk
	// |desired| Assignments to join against those as well.
	var it = LeftJoin{
		LenL: len(as.Items),
		LenR: len(as.Assignments),
		Compare: func(l, r int) int {
			return strings.Compare(itemAt(as.Items, l).ID, assignmentAt(as.Assignments, r).ItemID)
		},
	}
	for cur, ok := it.Next(); ok; cur, ok = it.Next() {
		// Remove any Assignments skipped between the last cursor iteration, and this
		// one. They must not have an associated Item (eg, it was deleted).
		if err := removeDeadAssignments(txn, as.KS, as.Assignments[lastCRE:cur.RightBegin]); err != nil {
			return err
		}
		lastCRE = cur.RightEnd

		var itemID, limit = itemAt(as.Items, cur.Left).ID, 0
		// Determine leading sub-slice of |desired| which are Assignments of |itemID|.
		for ; limit != len(desired) && desired[limit].ItemID == itemID; limit++ {
		}

		// Initialize |itemState|, computing the delta of current and |desired| Item Assignments.
		itemState.init(cur.Left, as.Assignments[cur.RightBegin:cur.RightEnd], desired[:limit])
		if err := itemState.constrainAndBuildOps(txn); err != nil {
			return err
		}
		desired = desired[limit:]
	}
	// Remove any trailing, dead Assignments.
	if err := removeDeadAssignments(txn, as.KS, as.Assignments[lastCRE:]); err != nil {
		return err
	}

	return nil
}

// removeDeadAssignments removes Assignments |asn|, after verifying each has no associated Item.
// This is a sanity check that our removal hasn't raced a re-creation of the Item.
func removeDeadAssignments(txn checkpointTxn, ks *keyspace.KeySpace, asn keyspace.KeyValues) error {
	for len(asn) != 0 {
		var itemID, limit = assignmentAt(asn, 0).ItemID, 1
		// Determine leading sub-slice of |asn| which are Assignments of |itemID|.
		for ; limit != len(asn) && assignmentAt(asn, limit).ItemID == itemID; limit++ {
		}
		// Verify Item does not exist.
		txn.If(clientv3.Compare(clientv3.CreateRevision(ItemKey(ks, itemID)), "=", 0))
		// Verify each Assignment has not changed, then remove it.
		for i := 0; i != limit; i++ {
			txn.If(modRevisionUnchanged(asn[i]))
			txn.Then(clientv3.OpDelete(string(asn[i].Raw.Key)))
		}
		if err := txn.Checkpoint(); err != nil {
			return err
		}
		asn = asn[limit:]
	}
	return nil
}

// modRevisionUnchanged returns a Cmp which verifies the key has not changed
// from the current KeyValue.
func modRevisionUnchanged(kv keyspace.KeyValue) clientv3.Cmp {
	return clientv3.Compare(clientv3.ModRevision(string(kv.Raw.Key)), "=", kv.Raw.ModRevision)
}

// checkpointTxn runs transactions. It's modeled on clientv3.Txn, but:
//  * It introduces "checkpoints", whereby many checkpoints may be grouped into
//    a smaller number of underlying Txns, while still providing a guarantee
//    that If/Thens of a checkpoint will be issued together in one Txn.
//  * It allows If and Then to be called multiple times.
//  * It removes Else, as incompatible with the checkpoint model. As such,
//    a Txn which does not succeed becomes an error.
type checkpointTxn interface {
	If(...clientv3.Cmp) checkpointTxn
	Then(...clientv3.Op) checkpointTxn
	Commit() (*clientv3.TxnResponse, error)

	// Checkpoint ensures that all If and Then invocations since the last
	// Checkpoint are issued in the same underlying Txn. It may partially
	// flush the transaction to Etcd.
	Checkpoint() error
}

// batchedTxn implements the checkpointTxn interface, potentially queuing across
// multiple transaction checkpoints and applying them together as a single
// larger transaction. This can alleviate network RTT, amortizing delay across
// many checkpoints.
type batchedTxn struct {
	// txnDo executes a OpTxn.
	txnDo func(txn clientv3.Op) (*clientv3.TxnResponse, error)
	// Completed checkpoints ready to flush.
	cmps []clientv3.Cmp
	ops  []clientv3.Op
	// Checkpoint currently being built.
	nextCmps []clientv3.Cmp
	nextOps  []clientv3.Op
	// Cmps which should be asserted on every underlying Txn.
	fixedCmps []clientv3.Cmp
	// Flags whether no operations have committed with this batchedTxn.
	noop bool
}

// newBatchedTxn returns a batchedTxn using the given Context and KV. It will
// apply |fixedCmps| on every underlying Txn it issues (eg, they needn't be added
// with If to each checkpoint).
func newBatchedTxn(ctx context.Context, kv clientv3.KV, fixedCmps ...clientv3.Cmp) *batchedTxn {
	if len(fixedCmps) >= maxTxnOps {
		panic("too many fixedCmps")
	}
	return &batchedTxn{
		txnDo: func(txn clientv3.Op) (*clientv3.TxnResponse, error) {
			if r, err := kv.Do(ctx, txn); err != nil {
				return nil, err
			} else {
				return r.Txn(), nil
			}
		},
		fixedCmps: fixedCmps,
		noop:      true,
	}
}

func (b *batchedTxn) If(c ...clientv3.Cmp) checkpointTxn {
	b.nextCmps = append(b.nextCmps, c...)
	return b
}
func (b *batchedTxn) Then(o ...clientv3.Op) checkpointTxn {
	b.nextOps = append(b.nextOps, o...)
	return b
}

func (b *batchedTxn) Checkpoint() error {
	if len(b.cmps) == 0 {
		b.cmps = append(b.cmps, b.fixedCmps...)
	}

	var nc, no = b.nextCmps, b.nextOps
	b.nextCmps, b.nextOps = b.nextCmps[:0], b.nextOps[:0]

	if lc, lo := len(b.cmps)+len(nc), len(b.ops)+len(no); lc > maxTxnOps || lo > maxTxnOps {
		if _, err := b.Commit(); err != nil {
			return err
		}
		b.cmps = append(b.cmps, b.fixedCmps...)
	}

	b.cmps = append(b.cmps, nc...)
	b.ops = append(b.ops, no...)
	return nil
}

func (b *batchedTxn) Commit() (*clientv3.TxnResponse, error) {
	if len(b.nextCmps) != 0 || len(b.nextOps) != 0 {
		panic("must call Checkpoint before Commit")
	}

	if r, err := b.txnDo(clientv3.OpTxn(b.cmps, b.ops, nil)); err != nil {
		return nil, err
	} else if !r.Succeeded {
		return r, fmt.Errorf("transaction checks did not succeed")
	} else {
		if len(b.ops) > 0 {
			b.noop = false
		}
		b.cmps, b.ops = b.cmps[:0], b.ops[:0]
		return r, nil
	}
}

func debugLogTxn(cmps []clientv3.Cmp, ops []clientv3.Op) {
	for _, c := range cmps {
		log.WithField("cmp", proto.CompactTextString((*etcdserverpb.Compare)(&c))).Info("cmp")
	}
	for _, o := range ops {
		if o.IsPut() {
			log.WithFields(log.Fields{
				"key":   string(o.KeyBytes()),
				"value": string(o.ValueBytes()),
			}).Info("put")
		} else if o.IsDelete() {
			log.WithFields(log.Fields{
				"key": string(o.KeyBytes()),
			}).Info("delete")
		}
	}
}

// maxTxnOps is set to etcd's `embed/config.go.DefaultMaxTxnOps`. Etcd allows
// configuration at runtime with --max-txn-ops. We assume the default and will
// error if a smaller value is used.
var maxTxnOps = 128
