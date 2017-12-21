package consensus

import (
	"errors"
	"sort"
	"strings"

	etcd "github.com/coreos/etcd/client"
	"github.com/coreos/etcd/store"
	"golang.org/x/net/context"
)

//go:generate mockery -inpkg -name=KeysAPI
//go:generate mockery -inpkg -name=Watcher

var (
	etcdUpsertOps = map[string]struct{}{
		store.CompareAndSwap: {},
		store.Create:         {},
		store.Get:            {},
		store.Set:            {},
		store.Update:         {},
	}
	etcdRemoveOps = map[string]struct{}{
		store.CompareAndDelete: {},
		store.Delete:           {},
		store.Expire:           {},
	}
)

// IsEtcdUpsertOp returns whether an etcd.Response.Action is an insert or update.
func IsEtcdUpsertOp(action string) bool {
	_, ok := etcdUpsertOps[action]
	return ok
}

// IsEtcdRemoveOp returns whether an etcd.Response.Action is a removal.
func IsEtcdRemoveOp(action string) bool {
	_, ok := etcdRemoveOps[action]
	return ok
}

// Child follows the hierarchy under |parent| for each successive path element
// in |names|, and returns the *etcd.Node of the leaf element if it exists, or
// nil otherwise.
//
// Precondition: |parent.Nodes| is recursively sorted.
func Child(parent *etcd.Node, names ...string) *etcd.Node {
	for len(names) != 0 && parent != nil {
		prefix := len(parent.Key) + 1

		ind := sort.Search(len(parent.Nodes), func(i int) bool {
			return parent.Nodes[i].Key[prefix:] >= names[0]
		})
		if ind < len(parent.Nodes) && parent.Nodes[ind].Key[prefix:] == names[0] {
			parent = parent.Nodes[ind]
			names = names[1:]
		} else {
			parent = nil
		}
	}
	return parent
}

// forEachChild invokes |cb| with the outer join of a name and its etcd.Node
// for |names| and |parent.Nodes|.
//
// Precondition: |names| is sorted, as are |parent.Nodes|. For |names| and
func forEachChild(parent *etcd.Node, names []string, cb func(string, *etcd.Node)) {
	nodes := parent.Nodes
	for len(nodes) != 0 || len(names) != 0 {
		// Integrity assertions.
		if len(names) > 1 && names[0] >= names[1] {
			panic("names not ordered")
		}
		if len(nodes) > 1 && nodes[0].Key >= nodes[1].Key {
			panic("nodes not ordered")
		}
		if len(names) != 0 && strings.IndexByte(names[0], '/') != -1 {
			panic("names cannot include slashes")
		}

		var nodeName string
		if len(nodes) != 0 {
			nodeName = nodes[0].Key[len(parent.Key)+1:]
		}

		if len(nodes) == 0 || (len(names) != 0 && nodeName > names[0]) {
			// Current name is not a child of |parent|.
			cb(names[0], &etcd.Node{Key: parent.Key + "/" + names[0]})
			names = names[1:]
		} else if len(names) == 0 || nodeName < names[0] {
			// Current node is not in |names|.
			cb(nodeName, nodes[0])
			nodes = nodes[1:]
		} else {
			// Current name and node match.
			cb(nodeName, nodes[0])
			nodes = nodes[1:]
			names = names[1:]
		}
	}
}

// FindNode performs a recursive search rooted at |node| to identify the
// |parent| of |key|, and the |index| where |key| exists or would be inserted.
// If a required parent of |key| does not exist, its insertion-point is
// returned.
//
// Precondition: |node| is recursively sorted.
func FindNode(node *etcd.Node, key string) (parent *etcd.Node, index int) {
	if len(key) <= len(node.Key) {
		return nil, -1
	}
	for {
		var ind int
		var intermediate string

		if slash := strings.IndexByte(key[len(node.Key)+1:], '/'); slash != -1 {
			// Search for the next intermediate directory node.
			intermediate = key[:len(node.Key)+1+slash]
			ind = sort.Search(len(node.Nodes), func(i int) bool {
				return node.Nodes[i].Key >= intermediate
			})
		} else {
			// No directories remain. Look for the exact key.
			ind = sort.Search(len(node.Nodes), func(i int) bool {
				return node.Nodes[i].Key >= key
			})
		}

		if ind < len(node.Nodes) && intermediate == node.Nodes[ind].Key {
			// Node is a parent. Iterate.
			node = node.Nodes[ind]
		} else {
			// |ind| is the node, or is it's insertion point.
			return node, ind
		}
	}
}

// PatchTree patches |tree| to reflect |response|, returning the new tree root
// (which may be |tree|). PatchTree() can be fed responses of a RetryWatcher()
// to provide an incrementally-built consistent view of a keyspace.
//
// Precondition: |tree| and |response| are recursively sorted.
func PatchTree(tree *etcd.Node, response *etcd.Response) (*etcd.Node, error) {
	if response.Action == store.Get && (tree == nil || tree.Key == response.Node.Key) {
		// Init or replace the entire tree.
		return response.Node, nil
	} else if tree == nil {
		return nil, errors.New("not a 'get', and tree is nil")
	} else if !strings.HasPrefix(response.Node.Key, tree.Key) {
		return tree, errors.New("expected response to be a tree subnode")
	}

	parent, ind := FindNode(tree, response.Node.Key)

	// Did we find the exact target node?
	if ind < len(parent.Nodes) && parent.Nodes[ind].Key == response.Node.Key {
		// Indicates an inconsistency.
		if response.Action == store.Create {
			return tree, errors.New("unexpected create")
		}

		if IsEtcdUpsertOp(response.Action) {
			parent.Nodes[ind] = response.Node
		} else if IsEtcdRemoveOp(response.Action) {
			// Remove the target node.
			if ind+1 < len(parent.Nodes) {
				copy(parent.Nodes[ind:], parent.Nodes[ind+1:])
			}
			parent.Nodes = parent.Nodes[:len(parent.Nodes)-1]
		} else {
			return tree, errors.New("unknown action")
		}
		return tree, nil
	}

	// The target doesn't exist. A non-upsert action indicates an inconsistency.
	if !IsEtcdUpsertOp(response.Action) {
		return tree, errors.New("non-upsert of missing key")
	}

	spliceNode := response.Node

	// Build required intermediate directories of the key.
	for subkey := response.Node.Key[len(parent.Key)+1:]; true; {
		if i := strings.LastIndexByte(subkey, '/'); i == -1 {
			break // No intermediate components remain.
		} else {
			subkey = subkey[:i]
		}

		spliceNode = &etcd.Node{
			Key:           parent.Key + "/" + subkey,
			Dir:           true,
			Nodes:         etcd.Nodes{spliceNode},
			CreatedIndex:  spliceNode.CreatedIndex,
			ModifiedIndex: spliceNode.ModifiedIndex,
		}
	}

	// Insert |spliceNode| into |parent| at |ind|.
	parent.Nodes = append(parent.Nodes, spliceNode)
	if ind+1 < len(parent.Nodes) {
		copy(parent.Nodes[ind+1:], parent.Nodes[ind:])
		parent.Nodes[ind] = spliceNode
	}

	return tree, nil
}

// CopyNode creates a deep-copy of |node|.
func CopyNode(node *etcd.Node) *etcd.Node {
	result := *node
	result.Nodes = CopyNodes(result.Nodes)
	return &result
}

// CopyNodes creates a deep-copy of |nodes|.
func CopyNodes(nodes etcd.Nodes) etcd.Nodes {
	if nodes == nil {
		return nil
	}
	result := make(etcd.Nodes, len(nodes))
	for i := range nodes {
		result[i] = CopyNode(nodes[i])
	}
	return result
}

// TerminalNodes returns all non-directory children under |node| via
// depth-first search (eg, maintaining key-order invariants of the tree).
func TerminalNodes(node *etcd.Node) etcd.Nodes {
	if node == nil {
		return []*etcd.Node{}
	} else if !node.Dir {
		return []*etcd.Node{node}
	}

	var result etcd.Nodes
	for _, n := range node.Nodes {
		result = append(result, TerminalNodes(n)...)
	}
	return result
}

type StringMap interface {
	Get(key string) string
}

// MapAdapter maps |node| into a StringMap which returns the Value of a nested
// '/'-separated path, or the empty string if the path does not exist.
func MapAdapter(node *etcd.Node) StringMap {
	return stringMapAdapter{node}
}

type stringMapAdapter struct{ *etcd.Node }

func (a stringMapAdapter) Get(key string) string {
	n := Child(a.Node, strings.Split(key, "/")...)
	if n != nil {
		return n.Value
	}
	return ""
}

// KeysAPI and Watcher duplicate interfaces of github.com/coreos/etcd/client
// by the same name for mock generation.
//
// TODO(johnny): We'd prefer to compose the etcd interfaces. However, we rely
// on mockery for mock generation, and first require a fix for
// https://github.com/vektra/mockery/issues/18.
type (
	KeysAPI interface {
		Get(ctx context.Context, key string, opts *etcd.GetOptions) (*etcd.Response, error)
		Set(ctx context.Context, key, value string, opts *etcd.SetOptions) (*etcd.Response, error)
		Delete(ctx context.Context, key string, opts *etcd.DeleteOptions) (*etcd.Response, error)
		Create(ctx context.Context, key, value string) (*etcd.Response, error)
		CreateInOrder(ctx context.Context, dir, value string, opts *etcd.CreateInOrderOptions) (*etcd.Response, error)
		Update(ctx context.Context, key, value string) (*etcd.Response, error)
		Watcher(key string, opts *etcd.WatcherOptions) etcd.Watcher
	}

	Watcher interface {
		Next(context.Context) (*etcd.Response, error)
	}
)
