package allocator

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/task"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Announcement manages a unique key which is "announced" to peers through Etcd,
// with an associated lease and a value which may be updated over time. It's
// useful for managing keys which simultaneously represent semantics of existence,
// configuration, and processing live-ness (such as allocator member keys).
type Announcement struct {
	Key      string
	Revision int64

	etcd *clientv3.Client
}

// Announce a key and value to etcd under the LeaseID, asserting the key doesn't
// already exist. If the key does exist, Announce will retry until it disappears
// (eg, due to a former lease timeout).
func Announce(etcd *clientv3.Client, key, value string, lease clientv3.LeaseID) *Announcement {
	for {
		var resp, err = etcd.Txn(context.Background()).
			If(clientv3.Compare(clientv3.Version(key), "=", 0)).
			Then(clientv3.OpPut(key, value, clientv3.WithLease(lease))).
			Commit()

		if err == nil && resp.Succeeded == false {
			err = fmt.Errorf("key exists")
		}

		if err == nil {
			return &Announcement{
				Key:      key,
				Revision: resp.Header.Revision,
				etcd:     etcd,
			}
		}

		log.WithFields(log.Fields{"err": err, "key": key}).
			Warn("failed to announce key (will retry)")

		time.Sleep(announceConflictRetryInterval)
	}
}

// Update the value of a current Announcement.
func (a *Announcement) Update(value string) error {
	var resp, err = a.etcd.Txn(context.Background()).
		If(clientv3.Compare(clientv3.ModRevision(a.Key), "=", a.Revision)).
		Then(clientv3.OpPut(a.Key, value, clientv3.WithIgnoreLease())).
		Commit()

	if err == nil && resp.Succeeded == false {
		err = fmt.Errorf("key modified or deleted externally (expected revision %d)", a.Revision)
	}
	if err == nil {
		a.Revision = resp.Header.Revision
	}
	return err
}

// SessionArgs are arguments of StartSession.
type SessionArgs struct {
	Etcd  *clientv3.Client
	Tasks *task.Group
	Spec  interface {
		Validate() error
		ZeroLimit()
		MarshalString() string
	}
	State    *State
	LeaseTTL time.Duration
	SignalCh <-chan os.Signal
	TestHook func(round int, isIdle bool)
}

// StartSession starts an allocator session. It:
// * Validates the MemberSpec.
// * Establishes an Etcd lease which conveys "liveness" of this member to its peers.
// * Announces the MemberSpec under the lease.
// * Loads the KeySpace as-of the announcement revision.
// * Queues tasks to the *task.Group which:
//   - Closes the Etcd lease on task.Group cancellation.
//   - Monitors SignalCh and zeros the MemberSpec ItemLimit on its signal.
//   - Runs the Allocate loop, cancelling the *task.Group on completion.
func StartSession(args SessionArgs) error {
	if err := args.Spec.Validate(); err != nil {
		return errors.WithMessage(err, "spec.Validate")
	}
	var lease, err = concurrency.NewSession(args.Etcd, concurrency.WithTTL(int(args.LeaseTTL.Seconds())))
	if err != nil {
		return errors.WithMessage(err, "establishing Etcd lease")
	}

	// Close |lease| when the task.Group is cancelled.
	args.Tasks.Queue("lease.Close", func() error {
		<-args.Tasks.Context().Done()
		return lease.Close()
	})

	var ann = Announce(args.Etcd, args.State.LocalKey, args.Spec.MarshalString(), lease.Lease())

	// Initialize the KeySpace at the announcement revision.
	if err = args.State.KS.Load(context.Background(), args.Etcd, ann.Revision); err != nil {
		return errors.WithMessage(err, "loading KeySpace")
	}

	// Monitor |SignalCh|, and re-announce a member limit of zero if it triggers.
	args.Tasks.Queue("zero member limit on signal", func() error {
		select {
		case sig := <-args.SignalCh:
			log.WithField("signal", sig).Info("caught signal")
		case <-args.Tasks.Context().Done():
			return nil
		}

		// Zero our advertised limit in Etcd. Upon seeing this, Allocator will
		// work to discharge all of our assigned items, and Allocate will exit
		// gracefully when none remain.
		args.Spec.ZeroLimit()
		return ann.Update(args.Spec.MarshalString())
	})

	// Serve the allocation loop. Cancel the task.Group when it finishes.
	args.Tasks.Queue("Allocate", func() error {
		defer args.Tasks.Cancel()

		var err = Allocate(AllocateArgs{
			Context:  args.Tasks.Context(),
			Etcd:     args.Etcd,
			State:    args.State,
			TestHook: args.TestHook,
		})
		if errors.Cause(err) == context.Canceled {
			err = nil
		}
		return err
	})

	return nil
}

var announceConflictRetryInterval = time.Second * 10
