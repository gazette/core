package allocator

import etcd "github.com/coreos/etcd/client"

// Allocator is an interface which performs distributed allocation of items.
type Allocator interface {
	KeysAPI() etcd.KeysAPI

	// Etcd path which roots shared state for this Context.
	PathRoot() string

	// A key uniquely identifying this Allocator within shared state.
	InstanceKey() string

	// The required number of replicas. Except in cases of failure, allocation
	// changes will not be made which would violate having at least this many
	// ready replicas of an item at all times.
	Replicas() int

	// Items which will be created if they do not exist. May be empty, and
	// additional items may be added at any time out-of-band (via creation of
	// a corresponding Etcd directory).
	FixedItems() []string

	// For |item| which is currently a local replica or master, returns a
	// representation of the local item processing state. State is shared with
	// other Allocators via this Allocator's |item| announcement in Etcd.
	ItemState(item string) string

	// For |state| of an item, which may be processed by another Allocator,
	// returns whether the item can safely be promoted at this time.
	ItemIsReadyForPromotion(item, state string) bool

	// Notifies Allocator of |route| for |item|. If |index| == -1, then Allocator
	// has no entry for |item|. Otherwise, |route.Entries[index]| is the entry
	// of this Allocator (and will have basename InstanceKey()). |tree| is given
	// as context: ItemRoute() will often wish to wish to inspect other state
	// within |tree| in response to a route change. Note that |route| or |tree|
	// must be copied if retained beyond this call
	ItemRoute(item string, route Route, index int, tree *etcd.Node)
}

// Route represents an agreed-upon, ordered set of responsible processes for an
// item.
type Route interface {
	// Index returns the index of |name| in |rt.Entries()|, or -1.
	Index(name string) int

	// IsReadyForHandoff returns whether all replicas are ready for the item
	// master to hand off item responsibility, without causing a violation of the
	// required replica count.
	IsReadyForHandoff(alloc Allocator) bool

	// Copy performs a deep-copy of route.
	Copy() Route

	// Item is the directory Node.
	Item() *etcd.Node

	// Entries are |Item().Nodes|, ordered on increasing CreatedIndex. 'Master' is
	// index 0, followed by item replicas. Depending on the target replica count,
	// there may be additional temporary entries which are neither master nor
	// replica (eg, due to a lost allocation race).
	Entries() etcd.Nodes
}
