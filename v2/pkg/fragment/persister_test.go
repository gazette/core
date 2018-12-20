package fragment

import (
	"context"
	"errors"
	"io/ioutil"
	"sync"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/etcdtest"
	"github.com/LiveRamp/gazette/v2/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/coreos/etcd/mvcc/mvccpb"
	gc "github.com/go-check/check"
)

type PersisterSuite struct{}

var _ = gc.Suite(&PersisterSuite{})
var obv testSpoolObserver

func (p *PersisterSuite) TestSpoolCompleteNotEnqueued(c *gc.C) {
	defer func(s func(ctx context.Context, spool Spool) error) { persistFn = s }(persistFn)
	persistFn = func(ctx context.Context, spool Spool) error { return nil }

	var ks = keyspace.NewKeySpace("/root", func(kv *mvccpb.KeyValue) (interface{}, error) {
		return allocator.Item{}, nil
	})
	var persister = NewPersister(PersisterOpts{KeySpace: ks})
	var spool = NewSpool("journal-1", &obv)

	// Empty spool is not enququed.
	persister.SpoolComplete(spool, true)
	c.Check(len(persister.qC), gc.Equals, 0)

	// Spool with no BackingStore is not enqueued.
	spool.End = int64(10)
	persister.SpoolComplete(spool, true)
	persister.mu.Lock()
	c.Check(len(persister.qC), gc.Equals, 0)
	persister.mu.Unlock()
}

func (p *PersisterSuite) TestSpoolCompleteNonPrimary(c *gc.C) {
	defer func(s func(ctx context.Context, spool Spool) error) { persistFn = s }(persistFn)
	persistFn = func(ctx context.Context, spool Spool) error {
		c.Error("spool should not be persisted")
		return nil
	}

	var ks = keyspace.NewKeySpace("/root", func(kv *mvccpb.KeyValue) (interface{}, error) {
		return allocator.Item{}, nil
	})
	var persister = NewPersister(PersisterOpts{KeySpace: ks})
	var spool = NewSpool("journal-1", &obv)
	spool.BackingStore = "file:///root/"
	applyAndCommit(&spool, "file:///root/")

	persister.SpoolComplete(spool, false)
	persister.mu.Lock()
	c.Check(len(persister.qC), gc.Equals, 1)
	persister.mu.Unlock()
}

func (p *PersisterSuite) TestSpoolCompletePrimary(c *gc.C) {
	var wg = sync.WaitGroup{}
	defer func(s func(ctx context.Context, spool Spool) error) { persistFn = s }(persistFn)
	persistFn = func(ctx context.Context, spool Spool) error {
		c.Check(spool.Fragment.Fragment, gc.DeepEquals, pb.Fragment{
			Journal:          "journal-1",
			Begin:            0,
			End:              12,
			Sum:              pb.SHA1SumOf("some content"),
			CompressionCodec: pb.CompressionCodec_NONE,
			BackingStore:     pb.FragmentStore("file:///root/"),
		})
		wg.Done()
		return nil
	}

	var ks = keyspace.NewKeySpace("/root", func(kv *mvccpb.KeyValue) (interface{}, error) {
		return allocator.Item{}, nil
	})
	var persister = NewPersister(PersisterOpts{KeySpace: ks})
	var obv testSpoolObserver
	var spool = NewSpool("journal-1", &obv)
	spool.BackingStore = pb.FragmentStore("file:///root/")
	applyAndCommit(&spool, "file:///root/")

	wg.Add(1)
	persister.SpoolComplete(spool, true)
	wg.Wait()
}

func (p *PersisterSuite) TestPersisterServe(c *gc.C) {
	defer func(s func(ctx context.Context, spool Spool) error) { persistFn = s }(persistFn)
	persistFn = func(ctx context.Context, spool Spool) error {
		c.Check(spool.Fragment.Fragment, gc.DeepEquals, pb.Fragment{
			Journal:          "journal-1",
			Begin:            0,
			End:              12,
			Sum:              pb.SHA1SumOf("some content"),
			CompressionCodec: pb.CompressionCodec_NONE,
			BackingStore:     "file:///root/",
		})
		return nil
	}

	var specFixture = &pb.JournalSpec{
		Fragment: pb.JournalSpec_Fragment{
			Stores: []pb.FragmentStore{"file:///root/"},
		},
	}
	var ks = keyspace.NewKeySpace("/root", func(kv *mvccpb.KeyValue) (interface{}, error) {
		return allocator.Item{
			ID:        "journal-1",
			ItemValue: specFixture,
		}, nil
	})
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()
	var _, err = client.Put(ctx, "/root/items/journal-1", "")
	c.Assert(err, gc.IsNil)
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	var timeChan = make(chan time.Time)
	var persister = NewPersister(PersisterOpts{
		KeySpace: ks,
		TimeChan: timeChan,
	})

	var spool = NewSpool("journal-1", &obv)
	spool.BackingStore = pb.FragmentStore("file:///root/")
	applyAndCommit(&spool, "file:///root/")

	go persister.Serve()
	persister.qA = append(persister.qA, spool)
	timeChan <- time.Time{}
	persister.Finish()
	// Ensure all queues are empty after rotation.
	persister.mu.Lock()
	c.Check(len(persister.qA)+len(persister.qB)+len(persister.qC), gc.Equals, 0)
	persister.mu.Unlock()
}

func (p *PersisterSuite) TestPersisterServeUpdateBackingStore(c *gc.C) {
	var tmpDir, err = ioutil.TempDir("", "")
	c.Assert(err, gc.IsNil)
	defer func(s string) { FileSystemStoreRoot = s }(FileSystemStoreRoot)
	FileSystemStoreRoot = tmpDir

	defer func(s func(ctx context.Context, spool Spool) error) { persistFn = s }(persistFn)
	persistFn = func(ctx context.Context, spool Spool) error {
		if spool.BackingStore == pb.FragmentStore("file:///root/invalid/") {
			return errors.New("something has gone wrong")
		}
		c.Check(spool.Fragment.Fragment, gc.DeepEquals, pb.Fragment{
			Journal:          "journal-1",
			Begin:            0,
			End:              12,
			Sum:              pb.SHA1SumOf("some content"),
			CompressionCodec: pb.CompressionCodec_NONE,
			BackingStore:     pb.FragmentStore("file:///root/"),
		})
		return nil
	}

	var specFixture = &pb.JournalSpec{
		Fragment: pb.JournalSpec_Fragment{
			Stores: []pb.FragmentStore{"file:///root/invalid/"},
		},
	}
	var ks = keyspace.NewKeySpace("/root", func(kv *mvccpb.KeyValue) (interface{}, error) {
		return allocator.Item{
			ID:        "journal-1",
			ItemValue: specFixture,
		}, nil
	})
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()
	_, err = client.Put(ctx, "/root/items/journal-1", "")
	c.Assert(err, gc.IsNil)
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	var timeChan = make(chan time.Time)
	var persister = NewPersister(PersisterOpts{
		KeySpace: ks,
		TimeChan: timeChan,
	})
	var spool = NewSpool("journal-1", &obv)
	spool.BackingStore = pb.FragmentStore("file:///root/invalid/")
	applyAndCommit(&spool, "file:///root/invalid/")

	go persister.Serve()
	persister.qA = append(persister.qA, spool)
	// Pass in two time.Time structs to the timeChan to ensure spool has been processed.
	timeChan <- time.Time{}
	timeChan <- time.Time{}
	// Failure to persist should cause spool to be reenqueued, however it is not guaranteed
	// which queue the spool will be in.
	persister.mu.Lock()
	c.Check(len(persister.qA)+len(persister.qB)+len(persister.qC), gc.Equals, 1)
	persister.mu.Unlock()

	// Update Jouranl Spec with valid store and retry persisting fragment.
	specFixture.Fragment.Stores[0] = "file:///root/"
	timeChan <- time.Time{}
	persister.Finish()
	persister.mu.Lock()
	c.Check(len(persister.qA)+len(persister.qB)+len(persister.qC), gc.Equals, 0)
	persister.mu.Unlock()
}

func applyAndCommit(spool *Spool, store string) {
	spool.applyContent(&pb.ReplicateRequest{
		Content:      []byte("some content"),
		ContentDelta: 0,
	})
	spool.applyCommit(&pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "journal-1",
			Begin:            0,
			End:              12,
			Sum:              pb.SHA1SumOf("some content"),
			CompressionCodec: pb.CompressionCodec_NONE,
			BackingStore:     pb.FragmentStore(store),
		}}, true)
}
