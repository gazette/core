package consensus

import (
	etcd "github.com/coreos/etcd/client"
	gc "github.com/go-check/check"

	"github.com/LiveRamp/gazette/consensus/allocator/mocks"
)

type RouteSuite struct{}

func (s *RouteSuite) TestOrdering(c *gc.C) {
	rt := s.fixture()

	c.Check(rt.entries[0].Key, gc.Equals, "/foo/bar/ccc")
	c.Check(rt.entries[1].Key, gc.Equals, "/foo/bar/aaa")
	c.Check(rt.entries[2].Key, gc.Equals, "/foo/bar/bbb")
}

func (s *RouteSuite) TestIndex(c *gc.C) {
	rt := s.fixture()

	c.Check(rt.Index("aaa"), gc.Equals, 1)
	c.Check(rt.Index("bbb"), gc.Equals, 2)
	c.Check(rt.Index("ccc"), gc.Equals, 0)
	c.Check(rt.Index("ddd"), gc.Equals, -1)
}

func (s *RouteSuite) TestReadyForHandoff(c *gc.C) {
	rt := s.fixture()
	alloc := &mocks.Allocator{}

	// No replicas required: always ready for handoff.
	alloc.On("Replicas").Return(0).Once()
	c.Check(rt.IsReadyForHandoff(alloc), gc.Equals, true)

	// More replicas than entries: not ready.
	alloc.On("Replicas").Return(100).Once()
	c.Check(rt.IsReadyForHandoff(alloc), gc.Equals, false)

	alloc.On("ItemIsReadyForPromotion", "bar", "ready").Return(true)
	alloc.On("ItemIsReadyForPromotion", "bar", "not-ready").Return(false)

	// Sufficient entries, but one is not ready.
	alloc.On("Replicas").Return(2).Once()
	c.Check(rt.IsReadyForHandoff(alloc), gc.Equals, false)

	// Required number of replicas are ready.
	alloc.On("Replicas").Return(1).Once()
	c.Check(rt.IsReadyForHandoff(alloc), gc.Equals, true)
}

func (s *RouteSuite) TestCopy(c *gc.C) {
	rt1 := s.fixture()
	rt2 := rt1.Copy()

	// Expect |rt2| has distinct entries storage from |rt1|.
	c.Check(rt1, gc.DeepEquals, rt2)
	rt1.entries[0], rt1.entries[1] = rt1.entries[1], rt1.entries[0]
	c.Check(rt1, gc.Not(gc.DeepEquals), rt2)
}

func (s *RouteSuite) fixture() route {
	item := &etcd.Node{
		Key: "/foo/bar",
		Nodes: []*etcd.Node{
			{Key: "/foo/bar/aaa", Value: "ready", CreatedIndex: 2},
			{Key: "/foo/bar/bbb", Value: "not-ready", CreatedIndex: 3},
			{Key: "/foo/bar/ccc", CreatedIndex: 1},
		},
	}

	rt := route{
		item:    item,
		entries: append([]*etcd.Node{}, item.Nodes...),
	}
	rt.init()

	return rt
}

var _ = gc.Suite(&RouteSuite{})
