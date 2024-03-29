package keyspace

import (
	"context"
	"testing"
	"time"

	epb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/etcdtest"
	gc "gopkg.in/check.v1"
)

type KeySpaceSuite struct{}

func (s *KeySpaceSuite) TestLoadAndWatch(c *gc.C) {
	var client = etcdtest.TestClient()
	var ctx, cancel = context.WithCancel(context.Background())

	defer etcdtest.Cleanup()

	// Fix some initial keys and values.
	_, err := client.Put(ctx, "/one", "1")
	c.Assert(err, gc.IsNil)
	_, err = client.Put(ctx, "/foo", "invalid value is logged and skipped")
	c.Assert(err, gc.IsNil)
	resp, err := client.Put(ctx, "/three", "3")
	c.Assert(err, gc.IsNil)
	_, err = client.Put(ctx, "/raced", "999")
	c.Assert(err, gc.IsNil)

	var ks = NewKeySpace("/", testDecoder)

	// Install an Observer and fix |obvCh| to be signaled on each call.
	var expectObserverCallCh = make(chan struct{}, 1)
	ks.Observers = append(ks.Observers, func() { expectObserverCallCh <- struct{}{} })

	// Load a Revision which is in the past (and doesn't yet reflect "/raced").
	c.Check(ks.Load(ctx, client, resp.Header.Revision), gc.IsNil)
	verifyDecodedKeyValues(c, ks.KeyValues, map[string]int{"/one": 1, "/three": 3})
	<-expectObserverCallCh

	go func() {
		for _, op := range []clientv3.Op{
			clientv3.OpPut("/two", "2"),
			clientv3.OpPut("/bar", "invalid key/value is also logged and skipped"),
			clientv3.OpDelete("/one"),
			clientv3.OpPut("/three", "4"),
			clientv3.OpPut("/foo", "5"), // Formerly invalid key/value is now consistent.
		} {
			var _, err = client.Do(ctx, op)
			c.Check(err, gc.IsNil)

			<-expectObserverCallCh
		}
		cancel()
	}()

	c.Check(ks.Watch(ctx, client), gc.Equals, context.Canceled)

	verifyDecodedKeyValues(c, ks.KeyValues,
		map[string]int{"/two": 2, "/three": 4, "/foo": 5, "/raced": 999})
}

func (s *KeySpaceSuite) TestHeaderChecking(c *gc.C) {
	var h = epb.ResponseHeader{
		ClusterId: 8675309,
		Revision:  100,
	}

	var other = epb.ResponseHeader{
		ClusterId: 8675309,
		MemberId:  111111,
		Revision:  123,
		RaftTerm:  232323,
	}
	c.Check(checkHeader(&h, other), gc.IsNil)

	other.MemberId = 222222
	c.Check(checkHeader(&h, other), gc.IsNil)

	other.Revision = 124
	other.MemberId = 333333
	other.RaftTerm = 3434343
	c.Check(checkHeader(&h, other), gc.IsNil)

	// ClusterId cannot change.
	other.Revision = 125
	other.ClusterId = 1337
	c.Check(checkHeader(&h, other), gc.ErrorMatches,
		`etcd ClusterID mismatch \(expected 8675309, got 1337\)`)
}

func (s *KeySpaceSuite) TestWatchResponseApply(c *gc.C) {
	var ks = NewKeySpace("/", testDecoder)
	// Normally the Header is populated during `Load`, so we simulate that setup here
	ks.Header = epb.ResponseHeader{ClusterId: 9999, Revision: 9}
	var resp = []clientv3.WatchResponse{{
		Header: epb.ResponseHeader{ClusterId: 9999, Revision: 10},
		Events: []*clientv3.Event{
			putEvent("/some/key", "99", 10, 10, 1),
			putEvent("/other/key", "100", 10, 10, 1),
		},
	}}

	c.Check(ks.Apply(resp...), gc.IsNil)
	verifyDecodedKeyValues(c, ks.KeyValues,
		map[string]int{"/some/key": 99, "/other/key": 100})

	// Key/value inconsistencies are logged but not returned as an error.
	resp = []clientv3.WatchResponse{{
		Header: epb.ResponseHeader{ClusterId: 9999, Revision: 11},
		Events: []*clientv3.Event{
			putEvent("/some/key", "101", 10, 11, 2),
			delEvent("/not/here", 11),
		},
	}}
	c.Check(ks.Apply(resp...), gc.IsNil)
	verifyDecodedKeyValues(c, ks.KeyValues,
		map[string]int{"/some/key": 101, "/other/key": 100})

	// Header inconsistencies fail the apply.
	resp = []clientv3.WatchResponse{{
		Header: epb.ResponseHeader{ClusterId: 10000, Revision: 12},
		Events: []*clientv3.Event{
			delEvent("/not/here", 11),
		},
	}}
	c.Check(ks.Apply(resp...), gc.ErrorMatches, `etcd ClusterID mismatch .*`)

	// A response without any events results in a panic
	resp = []clientv3.WatchResponse{{
		Header: epb.ResponseHeader{ClusterId: 9999, Revision: 12},
		Events: nil, // []*clientv3.Event{},
	}}
	c.Check(func() { ks.Apply(resp...) }, gc.PanicMatches, `runtime error: index out of range \[0\] with length 0`)

	// Events with out-of-sequence mod revisions will result in an error, even if the header revision seems valid
	resp = []clientv3.WatchResponse{{
		Header: epb.ResponseHeader{ClusterId: 9999, Revision: 13},
		Events: []*clientv3.Event{
			putEvent("/some/key", "bad", 11, 11, 1),
		},
	}}
	c.Check(ks.Apply(resp...), gc.ErrorMatches,
		`received watch response with first ModRevision 11, which is <= last Revision 11`)

	// WatchResponse with out-of-order inner events.
	resp = []clientv3.WatchResponse{{
		Header: epb.ResponseHeader{ClusterId: 9999, Revision: 13},
		Events: []*clientv3.Event{
			putEvent("/one/fish", "1", 13, 13, 1),
			putEvent("/two/fish", "2", 12, 12, 1),
		},
	}}
	c.Check(ks.Apply(resp...), gc.ErrorMatches,
		`received watch response with last ModRevision 12 less than first ModRevision 13`)

	// Multiple WatchResponses may be applied at once. Keys may be in any order,
	// and mutated multiple times within the batch apply.
	resp = []clientv3.WatchResponse{
		{
			Header: epb.ResponseHeader{ClusterId: 9999, Revision: 13},
			Events: []*clientv3.Event{
				putEvent("/aaaa", "1111", 12, 12, 1),
				putEvent("/bbbb", "2222", 12, 12, 1),
				putEvent("/cccc", "invalid", 12, 12, 1),
				putEvent("/to-delete", "0000", 12, 12, 1),
				putEvent("/bbbb", "3333", 12, 13, 2),
				putEvent("/cccc", "4444", 12, 13, 2),
			},
		},
		{
			Header: epb.ResponseHeader{ClusterId: 9999, Revision: 14},
			Events: []*clientv3.Event{
				putEvent("/aaaa", "5555", 12, 14, 2),
				delEvent("/to-delete", 14),
			},
		},
		{
			Header: epb.ResponseHeader{ClusterId: 9999, Revision: 16},
			Events: []*clientv3.Event{
				putEvent("/bbbb", "6666", 12, 16, 3),
				putEvent("/eeee", "7777", 16, 16, 1),
			},
		},
	}
	c.Check(ks.Apply(resp...), gc.IsNil)

	verifyDecodedKeyValues(c, ks.KeyValues,
		map[string]int{
			"/some/key":  101,
			"/other/key": 100,

			"/aaaa": 5555,
			"/bbbb": 6666,
			"/cccc": 4444,
			"/eeee": 7777,
		})
}

func (s *KeySpaceSuite) TestWaitForRevision(c *gc.C) {
	var ks = NewKeySpace("/", testDecoder)

	var ctx, cancel = context.WithCancel(context.Background())

	go func() {

		c.Check(ks.Apply(clientv3.WatchResponse{
			Header: epb.ResponseHeader{ClusterId: 123, Revision: 10},
			Events: []*clientv3.Event{
				putEvent("/key/10", "", 10, 10, 1),
			},
		}), gc.IsNil)

		c.Check(ks.Apply(clientv3.WatchResponse{
			Header: epb.ResponseHeader{ClusterId: 123, Revision: 99},
			Events: []*clientv3.Event{
				putEvent("/key/99", "", 99, 99, 1),
			},
		}), gc.IsNil)

		time.Sleep(time.Millisecond)

		c.Check(ks.Apply(clientv3.WatchResponse{
			Header: epb.ResponseHeader{ClusterId: 123, Revision: 100},
			Events: []*clientv3.Event{
				putEvent("/key/100", "", 100, 100, 1),
			},
		}), gc.IsNil)
	}()

	ks.Mu.RLock()
	defer ks.Mu.RUnlock()

	c.Check(ks.WaitForRevision(ctx, 100), gc.IsNil)
	c.Check(ks.Header.Revision, gc.Equals, int64(100))

	cancel()

	// Revision already met: returns cancelled.
	c.Check(ks.WaitForRevision(ctx, 99), gc.Equals, context.Canceled)
	// Future revision: doesn't block as context is cancelled.
	c.Check(ks.WaitForRevision(ctx, 101), gc.Equals, context.Canceled)
}

var _ = gc.Suite(&KeySpaceSuite{})

func Test(t *testing.T) { gc.TestingT(t) }

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
