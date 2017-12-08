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

// Route, route interface
// TODO(rupert): Gross name for temporary convenience
type Route interface {
	Index(name string) int
	IsReadyForHandoff(alloc Allocator) bool
	Item2() *etcd.Node
	Entries2() etcd.Nodes
	Copy() Route
}
