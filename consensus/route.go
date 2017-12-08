package consensus

import (
	"path"
	"sort"

	"github.com/LiveRamp/gazette/consensus/allocator"
	etcd "github.com/coreos/etcd/client"
)

// route implements allocator.Route
type route struct {
	item    *etcd.Node
	entries etcd.Nodes
}

// NewRoute initializes a new route from the |response| and |node|.
func NewRoute(response *etcd.Response, node *etcd.Node) allocator.Route {
	rt := route{
		item:    node,
		entries: append(etcd.Nodes{}, node.Nodes...), // Copy, as we'll re-order.
	}
	rt.init()
	return rt
}

// Index implements allocator.Route
func (rt route) Index(name string) int {
	prefix := len(rt.item.Key) + 1
	for ind := range rt.entries {
		if rt.entries[ind].Key[prefix:] == name {
			return ind
		}
	}
	return -1
}

// IsReadyForHandoff implements allocator.Route
func (rt route) IsReadyForHandoff(alloc allocator.Allocator) bool {
	if wanted := alloc.Replicas() + 1; len(rt.entries) < wanted {
		return false
	} else {
		item := path.Base(rt.item.Key)

		for j := 1; j != wanted; j++ {
			if !alloc.ItemIsReadyForPromotion(item, rt.entries[j].Value) {
				return false
			}
		}
		return true
	}
}

// Copy implements allocator.Route
func (rt route) Copy() allocator.Route {
	return route{
		item:    CopyNode(rt.item),
		entries: CopyNodes(rt.entries),
	}
}

// Item implements allocator.Route
func (rt route) Item() *etcd.Node {
	return rt.item
}

// Entries implements allocator.Route
func (rt route) Entries() etcd.Nodes {
	return rt.entries
}

func (rt route) init() {
	sort.Sort(createdIndexOrder(rt.entries))
}

// createdIndexOrder implements sort.Interface, ordering by increasing CreatedIndex.
type createdIndexOrder []*etcd.Node

func (h createdIndexOrder) Len() int           { return len(h) }
func (h createdIndexOrder) Less(i, j int) bool { return h[i].CreatedIndex < h[j].CreatedIndex }
func (h createdIndexOrder) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
