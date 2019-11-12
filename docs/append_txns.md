Design Brief: Append Transactions
=================================

Overview
--------

Journal append RPCs have several requirements:

* An append that reports success must be durable to the configured replication
  factor and availability zones.
* Appends may be of an arbitrary size, that may not be known ahead of time.
* Appends are all-or-nothing, and are frequently aborted mid-RPC (
  either explicitly by the client, or implicitly due to stream closure or timeout).
* A span of journal bytes assigned to a completed append must never again be
  re-assigned (eg, and journal offset is never written twice).
* Append RPCs may verify or update transactional journal "registers",
  or verify that it applies at an expected byte offset.
* Journal readers must observe only committed append RPCs.
* Latency and round-trips of the replication protocol must be minimized.

Under the hood, many broker components work together to provide these guarantees.

Components 
----------

Etcd is the sole source of truth for a journal's current topology.
Every broker maintains a local
[KeySpace](https://godoc.org/go.gazette.dev/core/keyspace)
which mirrors keys & values of Etcd, updated by a long-lived Etcd Watch.
At times a broker learn of a future Etcd revision from a peer,
in which case the broker must
[WaitForRevision](https://godoc.org/go.gazette.dev/core/keyspace#KeySpace.WaitForRevision)
of the KeySpace before proceeding.

A current set of journal assignments in Etcd define its
[Route](https://godoc.org/go.gazette.dev/core/broker/protocol#Route)
topology, which captures the replica peer set and current primary of the journal.
Topology disagreements can always be resolved by comparing the associated
Etcd revision, which provides a total ordering over Route changes. A local
[resolver](https://github.com/gazette/core/blob/master/broker/resolver.go#L16)
layers upon the KeySpace to provide mappings between a journal and
its current Route and effective Etcd revision. 

Brokers coordinate through a distributed
[allocator](https://godoc.org/go.gazette.dev/core/allocator)
to keep journal assignments up to date. The allocator will not allow
assignments to be removed if the current Route is not consistent or if
the journal would drop below the configured replication factor.
An implication is that (unless N>=R faults occur) there is always at least
one broker of a given Route that has participated in a previous journal
transaction.

Every span of journal content is defined by a content-addressed
[Fragment](https://godoc.org/go.gazette.dev/core/broker/protocol#Fragment),
and each replica of a journal maintains a
[Spool](https://godoc.org/go.gazette.dev/core/broker/fragment#Spool)
which is the replica's transactional "memory" of the journal. The Spool
maintains a Fragment currently being built, as well was a set of transactional
key/value pairs known as journal "registers" which may be operated on by
[AppendRequests](https://godoc.org/go.gazette.dev/core/broker/protocol#Fragment).
At any given time, just one RPC "owns" the replica Spool, coordinated through
Go channels. Other RPCs desiring the Spool must block.

Spools are mutated by applying a stream of
[ReplicateRequests](https://godoc.org/go.gazette.dev/core/broker/protocol#ReplicateRequest),
where each request either proposes a chunk of content to be appended,
or proposes that a specific Fragment be adopted. A commit is conveyed by a
Fragment proposal which incorporates new content chunks, while a roll-back is
a re-send of a prior adopted Fragment. Each replica & Spool independently
validate request transitions, including offsets and computed SHA1-sums.
Spools support a callback observer interface, which is invoked when a Fragment 
proposal is adopted. This callback is the means by which ongoing read RPCs are
made aware of new journal content.

Finally, a replication
[pipeline](https://github.com/gazette/core/blob/master/broker/pipeline.go)
manages a set of Replicate RPCs to each of the peers identified by a journal
Route. Replicate RPCs are bi-directional request/response streams, with a
long lifetime that spans across many individual Append RPCs. The pipeline and
its replication streams amortizes much of the setup and synchronization cost
of the transaction protocol. Typically, a pipeline is torn down and restarted
only when necessary, eg because the journal Route has changed. A constructed
pipeline also has ownership over the replica's Spool, and like the Spool,
at a given time just one goroutine "owns" the pipeline (and all others must
block). Pipeline ownership is further distinguished on "send" vs "receive",
which allows a pipeline to operate in full duplex mode.

Append State Machine
--------------------

Every Append RPC traverses through a state machine implemented in
[append_fsm.go](https://github.com/gazette/core/blob/master/broker/append_fsm.go).
This section describes the states and transitions of that machine.
Transitions to **stateError** are omitted for brevity: for example,
all blocking FSM states monitor the request Context, and will abort the
FSM appropriately.

1) **stateResolve**: performs resolution (or re-resolution) of the AppendRequest. If
    the request specifies a future Etcd revision, first block until that
    revision has been applied to the local KeySpace. This state may be
    re-entered multiple times.

    - Transition to **stateAwaitDesiredReplicas** if the local broker is not the current primary.
    - Transition to **stateStartPipeline** if the FSM already owns the pipeline.
    - Common case: transition to **stateAcquirePipeline**.

2) **stateAcquirePipeline**: performs a blocking acquisition of the exclusively-owned
    replica pipeline.

    - Transition to **stateResolve** if the prior resolution is invalidated while waiting.
    - Common case: The pipeline is now owned. Transition to **stateStartPipeline**.
 
3) **stateStartPipeline**: builds a pipeline by acquiring the exclusively-owned replica Spool,
    and then constructing a new pipeline (which starts Replicate RPCs to each Route peer).
    If the current pipeline is in an initialized state but has an older effective
    Route, it's torn down and a new one started. If the current pipeline Route is correct,
    this state is a no-op.

     - Common case: The pipeline is already in a good state, with an effective Route that
       matches the FSM's resolution. This state is an effective no-op.
       Transition to **stateUpdateAssignments**.
     - Transition to **stateResolve** if the resolved Route is invalidated while awaiting the Spool.
     - Otherwise the pipeline is uninitialized or at an older Route and must be built.
       Transition to **stateSendPipelineSync**.

4) **stateSendPipelineSync**: sends a synchronizing ReplicateRequest proposal to all
    replication peers, which includes the current Route, effective Etcd
    revision, and the proposed current Fragment to be extended. Each peer
    verifies the proposal and headers, and may either agree or indicate a conflict.

    - Transition to **stateRecvPipelineSync**.

5) **stateRecvPipelineSync**: reads synchronization acknowledgements from all replication peers.

     - Transition to **stateResolve** if any peer is aware of a non-equivalent Route at
       a later Etcd revision.
     - Transition to **stateSendPipelineSync** if any peer is aware of a larger
       journal append offset, or is unable to continue a current Fragment, in
       which case the current Fragment is closed & persisted and a new Fragment
       is begun at the end offset of the old. An implication is that a current
       Fragment is always closed and persisted when a new broker joins the topology.
     - Transition to **stateUpdateAssignments** if all peers agree with the proposal,
       indicating the pipeline is now synchronized.

6) **stateUpdateAssignments**: verifies and, if required, updates Etcd assignments to
    advertise the consistency of the present Route, which has been now been
    synchronized. Etcd assignment consistency advertises to the allocator that
    all replicas are consistent, and allows it to now remove undesired journal
    assignments.

     - Common case: Values reflect current Route and this state is a no-op.
       Transition to **stateAwaitDesiredReplicas**.
     - Otherwise, effect Etcd value updates to reflect the present Route.
       Transition to **stateResolve** at the Etcd operation revision.

7) **stateAwaitDesiredReplicas**: ensures the Route has the desired number of journal
    replicas. If there are too many, then the allocator has over-subscribed the
    journal in preparation for removing some of the current members -- possibly
    even the primary. It's expected that the allocator's removal of member(s) is
    imminent, and we should wait for the route to update rather than sending this
    append to N > R members (if primary) or to an old primary (if proxying).

     - If there are too many replicas, transition to **stateResolve** at the next
       unread Etcd revision.
     - If there are too few, transition to **stateError** (INSUFFICIENT_JOURNAL_BROKERS).
     - If we are not the local primary, transition to **stateProxy**, indicating the
       RPC must proxy to the indicated primary peer.
     - Common case: The Route has the proper number of replicas.
       Transition to **stateValidatePreconditions**.

8) **stateValidatePreconditions**: validates preconditions of the request. It ensures
    that current registers match the request's expectation, and if not it will
    fail the RPC with REGISTER_MISMATCH.
    
    It also validates next offset to be written.
    Appended data must always be written at the furthest known journal extent.
    Usually this will be the pipeline Spool offset. However if journal
    consistency is lost (due to too many broker or Etcd failures), a larger
    offset could exist in the fragment index.
    
    We don't attempt to automatically recover if consistency is lost. Instead
    the operator is required to craft an AppendRequest which explicitly
    captures the new, maximum journal offset to use.
    
    We do make an exception if the journal is not writable, in which case
    appendFSM can be used only for issuing zero-byte transaction barriers
    and there's no risk of double-writes to offsets. In particular this
    carve-out allows a journal to be a read-only view of a fragment store
    being written to by a separate & disconnected gazette cluster.
    
    Note request offsets may also be used outside of recovery, for example
    to implement at-most-once writes.

     - Transition to **stateResolve** if the Route changes while awaiting
       an initial fragment index refresh.
     - Transition to **stateError** if the request registers or offset don't match
       the request's expectation.
     - Common case: All precondition checks are successful.
       Transition to **stateStreamContent**.
 
9) **stateStreamContent**: called with each received content message or error
    from the Append RPC client. On its first call, it may "roll" the present
    Fragment to a new and empty Fragment (for example, if the Fragment is
    at its target length, or if the compression codec changed). Each non-empty
    content chunks is forwarded to all peers of the FSM's pipeline. An error
    of the client causes a roll-back to be sent to all peers. A final empty
    content chunk followed by an io.EOF causes a commit proposal to be sent
    to each peer, which (if adopted) extends the current Fragment with the
    client's appended content.

     - Transitions to itself with every non-empty client content chunk.
     - Transitions to **stateReadAcknowledgements** after sending a commit proposal or rollback.

10) **stateReadAcknowledgements**: releases ownership of the pipeline's send-side,
    enqueues itself for the pipeline's receive-side, and, upon its turn,
    reads responses from each replication peer.
    
    Recall that pipelines are full-duplex, and there may be other FSMs
    which completed onStreamContent before we did, and which have not yet read
    their acknowledgements from peers. Blocks on a cooperative pipeline "barrier"
    to determine when it's our turn to read our peer acknowledgements.

     - Transition to **stateFinished** once all peers acknowledge.

Discussion
----------

*A synchronized pipeline held by an FSM is a distributed and exclusive lock over
the capability to append to a journal*. That's because:

 * An invoked Replicate RPC obtains exclusive access to the replica Spool.
 * While holding that lock, the RPC waits for the primary to close the stream (or fail).
   Other RPCs of the replica will block obtaining the Spool, until it does so.
 * The allocator will never voluntarily remove assignments which are not synchronized.
 
Completing synchronization of a pipeline is thus a confirmation that there are no
other ongoing mutating RPCs of the journal, nor can there be (unless consistency is lost).

*Append RPCs require just one internal round-trip* if the common case is taken on all
state transitions. This round-trip awaits an explicit acknowledgement from each
replica that the commit proposal was accepted. While an RPC waits, many other RPCs
may be evaluated concurrently as the pipeline is full-duplex. If a replication
pipeline must be re-built, more round trips are of course required.

*Append RPCs don't buffer client content*. This means even very large appends
have minimal memory impacts on brokers, but does mean that multiple queued
appends can exhibit head-of-line blocking while awaiting the exclusive replication
pipeline. For this reason, the append FSM imposes a
[somewhat tight timeout](https://github.com/gazette/core/blob/master/broker/append_fsm.go#L17)
on the delivery of streamed client chunks. Clients are expected to be able to quickly
produce all content chunks and even then, for very long-distance appends, it
may make sense to having a buffering proxy that's closer to the broker.
