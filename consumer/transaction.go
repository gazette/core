package consumer

import (
	"bytes"
	"fmt"
	"io"
	"runtime/trace"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

// runTransactions runs consumer transactions. It consumes from the provided
// |readCh| and, when notified by |hintsCh|, occasionally stores recorded FSMHints.
// If |readCh| closes, runTransactions completes and fully commits a current
// transaction and then returns.
func runTransactions(s *shard, cp pc.Checkpoint, readCh <-chan EnvelopeOrError, hintsCh <-chan time.Time) error {
	var (
		realTimer = time.NewTimer(0)
		txnTimer  = txnTimer{
			C:     realTimer.C,
			Reset: realTimer.Reset,
			Stop:  realTimer.Stop,
			Now:   time.Now,
		}
		started = txnTimer.Now()
		prev    = transaction{
			checkpoint:        cp,
			commitBarrier:     client.FinishedOperation(nil),
			acks:              make(OpFutures),
			prevPrepareDoneAt: started,
			beganAt:           started,
			stalledAt:         started,
			prepareBeganAt:    started,
			prepareDoneAt:     started,
		}
		txn = transaction{}
	)
	<-realTimer.C // Timer starts as idle.

	// Begin by acknowledging (or re-acknowledging) messages published as part
	// of the most-recent recovered transaction checkpoint. This is a relaxed
	// form of txnAcknowledge(), as we allow recovered intents to name journals
	// that don't actually exist (presumably they were deleted in the meantime).
	for journal, ack := range cp.AckIntents {
		var op = client.NewAsyncOperation()
		prev.acks[op] = struct{}{}

		go func(req pb.AppendRequest, r *bytes.Reader) (_err error) {
			defer func() { op.Resolve(_err) }()

			if _, err := client.Append(s.ctx, s.ajc, req, r); err == client.ErrJournalNotFound {
				log.WithField("journal", req.Journal).
					Warn("discarding recovered ACK-intent of non-existent journal")
			} else if err != nil {
				return fmt.Errorf("writing recovered ACK intent for journal %s: %w", req.Journal, err)
			}
			return nil
		}(pb.AppendRequest{Journal: journal}, bytes.NewReader(ack))
	}

	for {
		select {
		case <-hintsCh:
			var hints, err = s.recovery.recorder.BuildHints()
			if err == nil {
				err = storeRecordedHints(s, hints)
			}
			if err != nil {
				return fmt.Errorf("storeRecordedHints: %w", err)
			}
			continue
		default:
			// Pass.
		}

		txnInit(s, &txn, &prev, readCh, txnTimer)
		if err := txnRun(s, &txn, &prev); err != nil {
			return err
		} else if txn.consumedCount == 0 {
			return nil // |readCh| has closed and drained.
		}
		prev, txn = txn, prev
	}
}

// transaction models a single consumer shard transaction.
type transaction struct {
	minDur, maxDur time.Duration          // Min/max processing durations. Set to -1 when elapsed.
	waitForAck     bool                   // Wait for ACKs of pending messages read this txn?
	barrierCh      <-chan struct{}        // Next barrier of previous transaction to resolve.
	readCh         <-chan EnvelopeOrError // Message source. Nil'd upon reaching |maxDur|.
	consumedCount  int                    // Number of acknowledged Messages consumed.
	consumedBytes  int64                  // Number of acknowledged Message bytes consumed.
	checkpoint     pc.Checkpoint          // Checkpoint upon the commit of this transaction.
	commitBarrier  OpFuture               // Barrier at which this transaction commits.
	acks           OpFutures              // ACKs of published messages, queued on |commitBarrier|.

	timer             txnTimer
	prevPrepareDoneAt time.Time // Time at which previous transaction finished preparing.
	beganAt           time.Time // Time at which first transaction message was processed.
	stalledAt         time.Time // Time at which |maxDur| elapsed without |priorCommittedCh| resolving.
	prepareBeganAt    time.Time // Time at which we began preparing to commit.
	prepareDoneAt     time.Time // Time at which we finished preparing to commit.
	committedAt       time.Time // Time at which |commitBarrier| resolved.
	ackedAt           time.Time // Time at which published |acks| resolved.
}

// txnInit initializes transaction |txn| in preparation to run.
func txnInit(s *shard, txn, prev *transaction, readCh <-chan EnvelopeOrError, timer txnTimer) {
	var spec = s.Spec()

	*txn = transaction{
		readCh:            readCh,
		acks:              make(OpFutures, len(prev.acks)),
		timer:             timer,
		minDur:            spec.MinTxnDuration,
		maxDur:            spec.MaxTxnDuration,
		waitForAck:        !spec.DisableWaitForAck,
		barrierCh:         prev.commitBarrier.Done(),
		prevPrepareDoneAt: prev.prepareDoneAt,
	}
}

// txnRun runs a single consumer transaction |txn| until it starts to commit.
func txnRun(s *shard, txn, prev *transaction) error {
	trace.Log(s.ctx, "txnRun", s.resolved.fqn)

	var done, err = txnStep(s, txn, prev)
	for !done && err == nil {
		done, err = txnStep(s, txn, prev)
	}

	if bf, ok := s.svc.App.(BeginFinisher); ok && !txn.beganAt.IsZero() {
		if err != nil {
			bf.FinishedTxn(s, s.store, client.FinishedOperation(err))
		} else {
			bf.FinishedTxn(s, s.store, txn.commitBarrier)
		}
	}
	return err
}

func txnBlocks(s *shard, txn *transaction) bool {
	// We always block to await a prior commit.
	if txn.barrierCh != nil {
		return true
	}
	// We don't block if the read channel has drained and no
	// transaction has been started (and now cannot ever start).
	if txn.readCh == nil && txn.consumedCount == 0 {
		return false
	}
	// Block if we haven't consumed messages yet.
	return txn.consumedCount == 0 ||
		// Or if the minimum batching duration hasn't elapsed.
		txn.minDur != -1 ||
		// Or if the maximum batching duration hasn't elapsed, we're still
		// reading messages, and a sequence started this transaction
		// awaits an ACK which we want to wait for.
		(txn.waitForAck && txn.readCh != nil && txn.maxDur != -1 && s.sequencer.HasPending())
}

// txnStep steps the transaction one time, and returns true iff it has started to commit.
func txnStep(s *shard, txn, prev *transaction) (bool, error) {

	// Attempt to consume a dequeued, committed message.
	if txn.readCh != nil && s.sequencer.Dequeued != nil {
		// Poll for a timer tick.
		select {
		case tick := <-txn.timer.C:
			return false, txnTick(s, txn, tick)
		default:
			// No ready tick.
		}

		if err := txnConsume(s, txn); err != nil {
			return false, err
		}
		return false, nil // We consumed one message.
	}

	if txnBlocks(s, txn) {
		select {
		case env, ok := <-txn.readCh:
			return false, txnRead(s, txn, prev, env, ok)
		case tick := <-txn.timer.C:
			return false, txnTick(s, txn, tick)
		case <-txn.barrierCh:
			return false, txnBarrierResolved(s, txn, prev)
		}
	} else {
		select {
		case env, ok := <-txn.readCh:
			return false, txnRead(s, txn, prev, env, ok)
		case tick := <-txn.timer.C:
			return false, txnTick(s, txn, tick)
		default:
			// Start to commit.
		}
	}

	if txn.consumedCount == 0 {
		return true, nil // |readCh| has closed and drained.
	} else if cp, err := txnStartCommit(s, txn); err != nil {
		return false, fmt.Errorf("txnStartCommit: %w", err)
	} else if err = txnAcknowledge(s, txn, cp); err != nil {
		return false, fmt.Errorf("txnAcknowledge: %w", err)
	}

	// If the timer is still running, stop and drain it.
	if txn.maxDur != -1 && !txn.timer.Stop() {
		<-txn.timer.C
	}
	return true, nil
}

func txnRead(s *shard, txn, prev *transaction, env EnvelopeOrError, ok bool) error {
	if !ok {
		txn.readCh = nil // Channel is closed, don't select it again.
		return nil
	} else if env.Error != nil {
		return fmt.Errorf("readMessage: %w", env.Error)
	}

	// DEPRECATED metrics to be removed:
	bytesConsumedTotal.Add(float64(env.End - env.Begin))
	readHeadGauge.WithLabelValues(env.Journal.Name.String()).Set(float64(env.End))
	// End DEPRECATED metrics.

	// |env| is read-uncommitted. Queue and act on its sequencing outcome.
	var outcome = s.sequencer.QueueUncommitted(env.Envelope)

	switch outcome {
	case message.QueueAckCommitReplay:

		var journal, from, to = s.sequencer.ReplayRange()
		trace.Logf(s.ctx, "StartReplay", "journal %s, range %d:%d", journal, from, to)

		var it message.Iterator
		if mp, ok := s.svc.App.(MessageProducer); ok {
			it = mp.ReplayRange(s, s.store, journal, from, to)
		} else {
			var rr = client.NewRetryReader(s.ctx, s.ajc, pb.ReadRequest{
				Journal:    journal,
				Offset:     from,
				EndOffset:  to,
				Block:      true,
				DoNotProxy: !s.ajc.IsNoopRouter(),
			})
			it = message.NewReadUncommittedIter(rr, s.svc.App.NewMessage)
		}
		s.sequencer.StartReplay(it)
		fallthrough

	case message.QueueAckCommitRing, message.QueueAckEmpty, message.QueueOutsideCommit:

		// Acknowledged messages are ready to dequeue.
		// We don't expect io.EOF here (we'll minimally dequeue |env| itself).
		if err := s.sequencer.Step(); err != nil {
			return fmt.Errorf("initial sequencer.Step: %w", err)
		}
		return nil

	case message.QueueContinueBeginSpan, message.QueueContinueExtendSpan:

		// A transactional message was enqueued,
		// and we expect to see a forthcoming commit or rollback.
		return nil

	case message.QueueAckRollback,
		message.QueueContinueAlreadyAcked,
		message.QueueContinueTxnClockLarger,
		message.QueueOutsideAlreadyAcked:

		// This was a duplicated message and there's no reason to expect it will
		// be followed by further messages.

		if txn.consumedCount != 0 {
			// We're inside a current transaction, and read progress will be
			// reported on its commit. Take no action.
		} else if txn.barrierCh != nil {
			// We're outside of a current transaction, but a previous one is still
			// committing. Extend it's checkpoint, which hasn't yet been reported
			// as progress, to include this no-op envelope. Note that this doesn't
			// change the persisted checkpoint (that's already been serialized).
			// It serves only to report progress for Stat RPCs.
			var s = prev.checkpoint.Sources[env.Journal.Name]
			s.ReadThrough = env.End
			prev.checkpoint.Sources[env.Journal.Name] = s
		} else {
			// We're currently idle, and must update progress to reflect that
			// |env| was read. If we didn't do this, then a Stat RPC
			// could stall indefinitely if its read-through offsets include a
			// message which doesn't cause a transaction to begin (such as a
			// duplicate ACK), and no further messages are forthcoming.
			signalProgress(s, func(readThrough, _ pb.Offsets) {
				readThrough[env.Journal.Name] = env.End
			})
		}
		return nil

	default:
		panic(fmt.Errorf("unhandled outcome %v", outcome))
	}
}

func txnConsume(s *shard, txn *transaction) error {
	// Does this message begin the transaction?
	if txn.consumedCount == 0 {
		trace.Log(s.ctx, "BeginTxn", s.resolved.fqn)

		if ba, ok := s.svc.App.(BeginFinisher); ok {
			// BeginTxn may block arbitrarily, for example by obtaining a
			// semaphore to constrain maximum concurrency.
			if err := ba.BeginTxn(s, s.store); err != nil {
				return fmt.Errorf("app.BeginTxn: %w", err)
			}
		}
		txn.beganAt = txn.timer.Now()
		txn.timer.Reset(txn.minDur)

		var delta = atomic.LoadInt64((*int64)(&s.svc.PublishClockDelta))
		s.clock.Update(txn.beganAt.Add(time.Duration(delta)))
	}

	var err = s.svc.App.ConsumeMessage(s, s.store, *s.sequencer.Dequeued, s.publisher)

	if err == ErrDeferToNextTransaction && txn.consumedCount == 0 {
		return fmt.Errorf("consumer transaction is empty, but application deferred the first message")
	} else if err == ErrDeferToNextTransaction {
		txn.readCh = nil // Stop reading further messages.
		return nil
	} else if err != nil {
		return fmt.Errorf("app.ConsumeMessage: %w", err)
	}

	txn.consumedCount++
	txn.consumedBytes += (s.sequencer.Dequeued.End - s.sequencer.Dequeued.Begin)

	if err := s.sequencer.Step(); err == io.EOF {
		// sequencer.Dequeued is now nil, and a further call to txnStep
		// will queue additional ready read-uncommitted messages.
	} else if err != nil {
		return fmt.Errorf("sequencer.Step: %w", err)
	}
	return nil // sequencer.Dequeued is the next message to consume.
}

func txnTick(s *shard, txn *transaction, tick time.Time) error {
	if tick.Before(txn.beganAt.Add(txn.minDur)) {
		panic("unexpected tick")
	}
	if txn.minDur == -1 && tick.Before(txn.beganAt.Add(txn.maxDur)) {
		panic("unexpected tick")
	}

	txn.minDur = -1 // Mark as completed.

	if tick.Before(txn.beganAt.Add(txn.maxDur)) {
		txn.timer.Reset(txn.beganAt.Add(txn.maxDur).Sub(tick))
		trace.Log(s.ctx, "txnTick(minDur)", s.resolved.fqn)
	} else {
		txn.maxDur = -1  // Mark as completed.
		txn.readCh = nil // Stop reading messages.

		if txn.barrierCh != nil {
			// If the prior transaction hasn't completed, we must wait until it
			// does but are now "stalled" as we cannot also read messages.
			txn.stalledAt = tick
			trace.Log(s.ctx, "txnTick(stalled)", s.resolved.fqn)
		} else {
			trace.Log(s.ctx, "txnTick(maxDur)", s.resolved.fqn)
		}
	}
	return nil
}

func txnBarrierResolved(s *shard, txn, prev *transaction) error {
	if !prev.ackedAt.IsZero() {
		panic("unexpected txnBarrierResolved")
	}
	var now = txn.timer.Now()

	if prev.committedAt.IsZero() {
		if prev.commitBarrier.Err() != nil {
			return fmt.Errorf("store.StartCommit: %w", prev.commitBarrier.Err())
		}
		prev.committedAt = now
	}

	// Find the next ACK append that hasn't finished.
	// It must resolve before |prev| is considered complete.
	txn.barrierCh = nil
	for ack := range prev.acks {
		select {
		case <-ack.Done(): // Already resolved?
			if ack.Err() != nil {
				return fmt.Errorf("prev.ack: %w", ack.Err())
			}
		default:
			txn.barrierCh = ack.Done()
		}
	}

	if txn.barrierCh != nil {
		trace.Log(s.ctx, "txnBarrier(partial)", s.resolved.fqn)
		return nil
	}

	// All barriers have finished. |prev| transaction is complete.
	trace.Log(s.ctx, "txnBarrier(done)", s.resolved.fqn)
	prev.ackedAt = now
	recordMetrics(s, prev)

	// Signal shard progress from results of |prev| transaction.
	signalProgress(s, func(readThrough, publishAt pb.Offsets) {
		for journal, source := range prev.checkpoint.Sources {
			readThrough[journal] = source.ReadThrough
		}
		for op := range prev.acks {
			if ack, ok := op.(*client.AsyncAppend); ok {
				publishAt[ack.Request().Journal] = ack.Response().Commit.End
			}
		}
	})

	return nil
}

func txnStartCommit(s *shard, txn *transaction) (pc.Checkpoint, error) {
	if txn.prepareBeganAt = txn.timer.Now(); txn.stalledAt.IsZero() {
		txn.stalledAt = txn.prepareBeganAt // We spent no time stalled.
	}

	trace.Log(s.ctx, "App.FinalizeTxn", s.resolved.fqn)
	var err = s.svc.App.FinalizeTxn(s, s.store, s.publisher)
	if err != nil {
		return pc.Checkpoint{}, fmt.Errorf("app.FinalizeTxn: %w", err)
	}

	var offsets, states = s.sequencer.Checkpoint(messageSequencerPruneHorizon)
	var bca = pc.BuildCheckpointArgs{ReadThrough: offsets, ProducerStates: states}
	if bca.AckIntents, err = s.publisher.BuildAckIntents(); err != nil {
		return pc.Checkpoint{}, fmt.Errorf("publisher.BuildAckIntents: %w", err)
	}
	txn.checkpoint = pc.BuildCheckpoint(bca)

	// Collect pending journal writes before we start to commit. We'll require
	// that the Store wait on all |waitFor| operations before it commits, to
	// ensure that writes driven by messages of the transaction have completed
	// before we could possibly persist their acknowledgments, or commit offsets
	// which step past those messages.
	var waitFor = s.ajc.PendingExcept(s.recovery.log)

	trace.Log(s.ctx, "store.StartCommit", s.resolved.fqn)
	var barrier = s.store.StartCommit(s, txn.checkpoint, waitFor)

	// If StartCommit returned an OpFuture which is a pre-resolved error,
	// handle it as an immediate and synchronous error. This avoids raced
	// calls to Application.ConsumeMessage when the Application has already
	// failed.
	select {
	case <-barrier.Done():
		if barrier.Err() != nil {
			return pc.Checkpoint{}, fmt.Errorf("store.StartCommit: %w", barrier.Err())
		}
	default:
	}

	txn.commitBarrier = barrier
	txn.prepareDoneAt = txn.timer.Now()

	return txn.checkpoint, nil
}

func txnAcknowledge(s *shard, txn *transaction, cp pc.Checkpoint) error {
	// The transaction is committed when |commitBarrier| resolves. At that
	// point any reader of the log must see our checkpoint.
	var waitFor = OpFutures{txn.commitBarrier: {}}

	// After |commitBarrier| resolves, publish our ACKs to unblock downstream
	// readers to process messages we published during this transaction. If we
	// fault after |commitBarrier| resolves and before we send all ACKs, the
	// next process to recover from our log will re-send those ACKs, so they're
	// guaranteed to eventually be received.
	for journal, ack := range cp.AckIntents {
		var aa = s.ajc.StartAppend(pb.AppendRequest{Journal: journal}, waitFor)
		_, _ = aa.Writer().Write(ack)

		if err := aa.Release(); err != nil {
			return fmt.Errorf("writing ACK: %w", err)
		}
		txn.acks[aa] = struct{}{}
	}
	return nil
}

// signalProgress of the shard in reading or publishing journals, as an atomic
// update of shard-associated metadata which also awakes any blocking
// tasks that process progress updates (such as the Shard Stat API).
func signalProgress(s *shard, cb func(readThrough, publishAt pb.Offsets)) {
	s.progress.Lock()
	defer s.progress.Unlock()

	cb(s.progress.readThrough, s.progress.publishAt)

	close(s.progress.signalCh)
	s.progress.signalCh = make(chan struct{})
}

// recordMetrics of a fully completed transaction.
func recordMetrics(s *shard, txn *transaction) {

	// DEPRECATED metrics, to be removed:
	txCountTotal.Inc()
	txMessagesTotal.Add(float64(txn.consumedCount))

	txSecondsTotal.Add(txn.committedAt.Sub(txn.beganAt).Seconds())
	txConsumeSecondsTotal.Add(txn.stalledAt.Sub(txn.beganAt).Seconds())
	txStalledSecondsTotal.Add(txn.prepareBeganAt.Sub(txn.stalledAt).Seconds())
	txFlushSecondsTotal.Add(txn.prepareDoneAt.Sub(txn.prepareBeganAt).Seconds())
	txSyncSecondsTotal.Add(txn.committedAt.Sub(txn.prepareDoneAt).Seconds())
	// End DEPRECATED metrics.

	shardTxnTotal.WithLabelValues(s.FQN()).Inc()
	shardReadMsgsTotal.WithLabelValues(s.FQN()).Add(float64(txn.consumedCount))
	shardReadBytesTotal.WithLabelValues(s.FQN()).Add(float64(txn.consumedBytes))
	for journal, source := range txn.checkpoint.Sources {
		shardReadHeadGauge.
			WithLabelValues(s.FQN(), journal.String()).
			Set(float64(source.ReadThrough))
	}

	var (
		durNotRunning    = txn.beganAt.Sub(txn.prevPrepareDoneAt)
		durConsuming     = txn.stalledAt.Sub(txn.beganAt)
		durStalled       = txn.prepareBeganAt.Sub(txn.stalledAt)
		durPreparing     = txn.prepareDoneAt.Sub(txn.prepareBeganAt)
		durCommitting    = txn.committedAt.Sub(txn.prepareDoneAt)
		durAcknowledging = txn.ackedAt.Sub(txn.committedAt)
	)

	log.WithFields(log.Fields{
		"id":              s.Spec().Id,
		"10NotRunning":    durNotRunning,
		"20Consuming":     durConsuming,
		"30Stalled":       durStalled,
		"40Prepare":       durPreparing,
		"50Committing":    durCommitting,
		"60Acknowledging": durAcknowledging,
		"messages":        txn.consumedCount,
		"bytes":           txn.consumedBytes,
	}).Debug("transaction metrics")

	// Phases which run synchronously within the transaction loop.
	shardTxnPhaseSecondsTotal.WithLabelValues(s.FQN(), "10-not-running", "sync").Add(durNotRunning.Seconds())
	shardTxnPhaseSecondsTotal.WithLabelValues(s.FQN(), "20-consuming", "sync").Add(durConsuming.Seconds())
	shardTxnPhaseSecondsTotal.WithLabelValues(s.FQN(), "30-stalled", "sync").Add(durStalled.Seconds())
	shardTxnPhaseSecondsTotal.WithLabelValues(s.FQN(), "40-preparing", "sync").Add(durPreparing.Seconds())
	// Phases which run asynchronously, in parallel with later transaction.
	shardTxnPhaseSecondsTotal.WithLabelValues(s.FQN(), "50-committing", "async").Add(durCommitting.Seconds())
	shardTxnPhaseSecondsTotal.WithLabelValues(s.FQN(), "60-acknowledging", "async").Add(durAcknowledging.Seconds())
}

// txnTimer is a time.Timer which can be mocked within unit tests.
type txnTimer struct {
	C     <-chan time.Time
	Reset func(time.Duration) bool
	Stop  func() bool
	Now   func() time.Time
}

// ErrDeferToNextTransaction may be returned by Application.ConsumeMessage to
// indicate that processing of the Envelope should be deferred to a following
// transaction. It's an error to return it on the very first message of the
// transaction (it cannot be empty).
var ErrDeferToNextTransaction = fmt.Errorf("consumer application deferred message")
