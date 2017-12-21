package consensus

import (
	"context"
	"flag"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"

	etcd "github.com/coreos/etcd/client"
	gc "github.com/go-check/check"

	"github.com/LiveRamp/gazette/envflag"
	"github.com/LiveRamp/gazette/envflagfactory"
)

type AllocRunSuite struct {
	// Allocator fields.
	etcdClient etcd.Client
	pathRoot   string
	replicas   int
	fixedItems []string

	// Tracked state, for verification.
	routesMu sync.Mutex
	routes   map[string]Route

	notifyCh chan notify
}

func (s *AllocRunSuite) SetUpSuite(c *gc.C) {
	if testing.Short() {
		c.Skip("skipping allocator integration tests in short mode")
	}

	var etcdEndpoint = envflagfactory.NewEtcdServiceEndpoint()

	envflag.CommandLine.Parse()
	flag.Parse()

	s.etcdClient, _ = etcd.New(etcd.Config{
		Endpoints: []string{"http://" + *etcdEndpoint}})

	// Skip suite if Etcd is not available.
	if _, err := s.KeysAPI().Get(context.Background(), "/",
		&etcd.GetOptions{Recursive: false}); err != nil {
		c.Skip(err.Error())
	}

	s.notifyCh = make(chan notify, 1)
}

func (s *AllocRunSuite) SetUpTest(c *gc.C) {
	s.pathRoot = "/tests/" + c.TestName()
	s.replicas = 0 // Overridden by some tests.
	s.fixedItems = []string{"bar", "baz", "foo"}
	s.routes = make(map[string]Route)

	// Clear a previous test Etcd directory (if it exists).
	s.KeysAPI().Delete(context.Background(), s.pathRoot, &etcd.DeleteOptions{Recursive: true})
}

func (s *AllocRunSuite) TestSingle(c *gc.C) {
	var alloc = newTestAlloc(s, "my-key")
	alloc.inspectCh = make(chan func(*etcd.Node))

	go alloc.createAndRun(c)

	// Stage 1: Expect that we acquired master locks on all items.
	s.wait(waitFor{idle: []string{"my-key"}})

	for _, item := range s.fixedItems {
		c.Check(s.routes[item].Entries, gc.HasLen, 1)
		c.Check(s.routes[item].Index("my-key"), gc.Equals, 0)
	}

	// Perform the same check via the InspectChan mechanism.
	var inspectionDoneCh = make(chan struct{})

	alloc.inspectCh <- func(tree *etcd.Node) {
		var i int
		WalkItems(tree, nil, func(name string, route Route) {
			c.Check(s.fixedItems[i], gc.Equals, name)
			c.Check(route.Index("my-key"), gc.Equals, 0)
			i++
		})
		c.Check(i, gc.Equals, len(s.fixedItems))
		close(inspectionDoneCh)
	}
	<-inspectionDoneCh

	// Interlude: Expect that an attempt to Create the same Allocator fails.
	c.Check(Create(testAllocator{AllocRunSuite: s, instanceKey: "my-key"}),
		gc.Equals, ErrAllocatorInstanceExists)

	c.Check(CancelItem(alloc, "bar"), gc.IsNil)

	// Stage 2: Expect that the cancelled item is re-acquired.
	s.wait(waitFor{idle: []string{"my-key"}})

	c.Check(s.routes["bar"].Entries, gc.HasLen, 1)
	c.Check(s.routes["bar"].Index("my-key"), gc.Equals, 0)
	c.Check(Cancel(alloc), gc.IsNil)

	// Stage 2: Allocator exited. Expect all item entries were released.
	s.wait(waitFor{exit: []string{"my-key"}})

	for _, item := range s.fixedItems {
		c.Check(s.routes[item].Entries, gc.HasLen, 0)
	}
}

func (s *AllocRunSuite) TestHandlingOfNestedItemDirectories(c *gc.C) {
	s.fixedItems = []string{}
	s.replicas = 1

	_, err := s.KeysAPI().Set(context.Background(), s.PathRoot()+"/items/some/nested/item", "",
		&etcd.SetOptions{Dir: true})
	c.Check(err, gc.IsNil)

	alloc := newTestAlloc(s, "my-key")
	go alloc.createAndRun(c)

	// Stage 1: Expect that we acquired a replica lock on "some" (We interpret
	// "nested" to be the master lock, due to its earlier creation index).
	// Expect that nothing else blew up.
	s.wait(waitFor{idle: []string{"my-key"}})

	c.Check(s.routes, gc.HasLen, 1)
	c.Check(s.routes["some"].Entries, gc.HasLen, 2)
	c.Check(s.routes["some"].Index("my-key"), gc.Equals, 1)

	c.Check(Cancel(alloc), gc.IsNil)
	s.wait(waitFor{exit: []string{"my-key"}})
}

func (s *AllocRunSuite) TestAsyncItemCreationAndRemoval(c *gc.C) {
	s.replicas = 1

	alloc1 := newTestAlloc(s, "alloc-1")
	alloc2 := newTestAlloc(s, "alloc-2")
	go alloc1.createAndRun(c)
	go alloc2.createAndRun(c)

	// Stage 1: Expect fixed items have master & replica entries.
	s.wait(waitFor{idle: []string{"alloc-1", "alloc-2"}})

	c.Check(s.routes, gc.HasLen, 3)
	for _, route := range s.routes {
		c.Check(route.Entries, gc.HasLen, 2)
	}

	// Create a new item (by adding its directory).
	_, err := s.KeysAPI().Set(context.Background(), s.PathRoot()+"/items/a-new-item", "",
		&etcd.SetOptions{Dir: true})
	c.Check(err, gc.IsNil)

	// Stage 2: Expect new item is now assigned as well.
	s.wait(waitFor{idle: []string{"alloc-1", "alloc-2"}})

	c.Check(s.routes, gc.HasLen, 4)
	c.Check(s.routes["a-new-item"].Entries, gc.HasLen, 2)

	delete(s.routes, "a-new-item") // Expect ItemRoute("a-new-item") isn't invoked again.

	// Delete both "a-new-item" and fixed item "bar".
	_, err = s.KeysAPI().Delete(context.Background(), s.PathRoot()+"/items/a-new-item",
		&etcd.DeleteOptions{Dir: true, Recursive: true})
	c.Check(err, gc.IsNil)

	_, err = s.KeysAPI().Delete(context.Background(), s.PathRoot()+"/items/bar",
		&etcd.DeleteOptions{Dir: true, Recursive: true})
	c.Check(err, gc.IsNil)

	// Stage 3: Expect fixed item "bar" is re-created, and all are assigned.
	s.wait(waitFor{idle: []string{"alloc-1", "alloc-2"}})

	c.Check(s.routes, gc.HasLen, 3)
	c.Check(s.routes["bar"].Entries, gc.HasLen, 2)

	// Shut down both allocators.
	s.replicas = 0
	c.Check(Cancel(alloc1), gc.IsNil)
	c.Check(Cancel(alloc2), gc.IsNil)

	s.wait(waitFor{exit: []string{"alloc-1", "alloc-2"}})

	// Stage 3: Allocator exited. All items were released.
	c.Check(s.routes, gc.HasLen, 3)
	for _, route := range s.routes {
		c.Check(route.Entries, gc.HasLen, 0)
	}
}

func (s *AllocRunSuite) TestDeadlockSimple(c *gc.C) {
	s.replicas = 1
	s.fixedItems = []string{"item-1", "item-2"}

	var alloc1 = newTestAlloc(s, "alloc-1")
	var alloc2 = newTestAlloc(s, "alloc-2")

	go alloc1.createAndRun(c)
	go alloc2.createAndRun(c)

	// Stage 1: |alloc1| & |alloc2| aquire master and replica on item-1, item-2.
	s.wait(waitFor{idle: []string{"alloc-1", "alloc-2"}})
	for _, item := range s.fixedItems {
		c.Check(s.routes[item].Entries.Len(), gc.Equals, 2)
	}

	// Stage 2: Start |alloc3|, *then* create a third item (order is important).
	var alloc3 = newTestAlloc(s, "alloc-3")
	c.Assert(Create(alloc3), gc.IsNil)

	// Create a new item (by adding its directory).
	var _, err = s.KeysAPI().Set(context.Background(), s.PathRoot()+"/items/a-new-item", "",
		&etcd.SetOptions{Dir: true})
	c.Check(err, gc.IsNil)

	go alloc3.run(c)
	s.wait(waitFor{idle: []string{"alloc-1", "alloc-2", "alloc-3"}})

	// Expect that we did not deadlock: that all items are fully replicated
	for _, route := range s.routes {
		c.Check(route.Entries.Len(), gc.Equals, 2)
	}

	s.replicas = 0 // Allow allocators to exit.
	for _, a := range []Allocator{alloc1, alloc2, alloc3} {
		c.Check(Cancel(a), gc.IsNil)
	}
	s.wait(waitFor{exit: []string{"alloc-1", "alloc-2", "alloc-3"}})
}

func (s *AllocRunSuite) TestDeadlockExtended(c *gc.C) {
	if testing.Short() {
		c.Skip("skipping in --short mode")
		return
	}

	s.replicas = 2

	// Create items such that every desired master & replica slot of every
	// allocator must be utilized for each item to be fully replicated.
	s.fixedItems = s.fixedItems[:0]
	for i := 0; i != 20*(s.replicas+1)*4; i++ {
		s.fixedItems = append(s.fixedItems, fmt.Sprintf("item-%04d", i))
	}

	var allocs []testAllocator
	var names []string

	// Start four allocators, and wait for them to converge.
	for i := 0; i != 4; i++ {
		var name = fmt.Sprintf("alloc-%02d", i)
		var alloc = newTestAlloc(s, name)
		go alloc.createAndRun(c)

		allocs = append(allocs, alloc)
		names = append(names, name)
	}
	s.wait(waitFor{idle: names})

	c.Log("simulating rolling update of alloc-0")
	c.Check(Cancel(allocs[0]), gc.IsNil)
	var actions = s.wait(waitFor{exit: names[:1], idle: names[1:]})

	go allocs[0].createAndRun(c)
	actions += s.wait(waitFor{idle: names})

	// Expect all items are fully replicated.
	for _, item := range s.fixedItems {
		c.Check(s.routes[item].Entries.Len(), gc.Equals, 3)
	}

	// Regression check: experimentally |actions| is in range [2000-2400].
	c.Check(actions < 2500, gc.Equals, true)
	c.Log("actions: ", actions)

	s.replicas = 0 // Allow allocators to exit.
	for _, a := range allocs {
		c.Check(Cancel(a), gc.IsNil)
	}
	s.wait(waitFor{exit: names})
}

func (s *AllocRunSuite) TestAllocatorHandoff(c *gc.C) {
	s.replicas = 1

	alloc1 := newTestAlloc(s, "alloc-1")
	alloc2 := newTestAlloc(s, "alloc-2")
	alloc3 := newTestAlloc(s, "alloc-3")

	go alloc1.createAndRun(c)

	// Stage 1: |alloc1| has acquired master on all items.
	s.wait(waitFor{idle: []string{"alloc-1"}})
	for _, item := range s.fixedItems {
		c.Check(s.routes[item].Index("alloc-1"), gc.Equals, 0)
	}
	go alloc2.createAndRun(c)

	// Stage 2: Mastered items are split between both allocators.
	s.wait(waitFor{idle: []string{"alloc-1", "alloc-2"}})

	c.Check(s.routes, gc.HasLen, len(s.fixedItems))
	for _, route := range s.routes {
		c.Check(route.Entries, gc.HasLen, 2)
	}
	c.Check(s.masterCounts(), gc.DeepEquals, map[string]int{
		"alloc-1": 2, // Has larger share, because it was first.
		"alloc-2": 1,
	})
	c.Check(Cancel(alloc1), gc.IsNil)
	go alloc3.createAndRun(c)

	// Stage 3. First allocator has exited. Items are still split.
	s.wait(waitFor{exit: []string{"alloc-1"}, idle: []string{"alloc-2", "alloc-3"}})

	for _, route := range s.routes {
		c.Check(route.Entries, gc.HasLen, 2)
	}
	c.Check(s.masterCounts(), gc.DeepEquals, map[string]int{
		"alloc-2": 2, // Has larger share, because it was first.
		"alloc-3": 1,
	})
	c.Check(Cancel(alloc2), gc.IsNil)

	// Stage 4: Second allocator has exited. Third allocator handles all items.
	s.wait(waitFor{exit: []string{"alloc-2"}, idle: []string{"alloc-3"}})

	for _, route := range s.routes {
		c.Check(route.Entries, gc.HasLen, 1)
	}
	c.Check(s.masterCounts(), gc.DeepEquals, map[string]int{"alloc-3": 3})

	s.replicas = 0 // Required for allocator to voluntarily exit.
	c.Check(Cancel(alloc3), gc.IsNil)

	// Stage 5: All item locks are released. Allocators have exited.
	s.wait(waitFor{exit: []string{"alloc-3"}})

	for _, item := range s.fixedItems {
		c.Check(s.routes[item].Entries, gc.HasLen, 0)
	}
}

func (t *AllocRunSuite) masterCounts() map[string]int {
	var counts = make(map[string]int)
	for _, item := range t.fixedItems {
		// Group by allocator name, and count.
		key := t.routes[item].Entries[0].Key
		counts[key[strings.LastIndexByte(key, '/')+1:]] += 1
	}
	return counts
}

// Helper which uses testNotifier hooks in Allocate(), along with tracking
// exits of Allocate() calls from createAndRun(), to determine when a set of concurrent
// Allocate() calls are fully idle and/or exited. This is done by returning only
// after all instances have become idle (have no actions to perform) at the
// largest "acted" index returned by any instance.
func (s *AllocRunSuite) wait(desc waitFor) int {
	var maxIndex uint64 = 1
	var actions int

	state := make(map[string]uint64)
	for _, n := range desc.idle {
		state[n] = 0
	}
	for _, n := range desc.exit {
		state[n] = math.MaxUint64 // Can never be matched. Must be deleted.
	}

	for {
		if msg := <-s.notifyCh; msg.key == "" {
			// ActedAt() notification.
			if msg.index > maxIndex {
				maxIndex = msg.index
			}
			actions += 1
			continue
		} else if msg.exit {
			// Exit notification.
			delete(state, msg.key)
		} else if msg.index > state[msg.key] {
			// IdleAt() notification.
			state[msg.key] = msg.index
		}

		// Return iff all allocators are at |maxIndex|.
		done := true
		for _, ind := range state {
			if ind != maxIndex {
				done = false
				break
			}
		}
		if done {
			return actions
		}
	}
}

// Partial Allocator implementation.
func (s *AllocRunSuite) KeysAPI() etcd.KeysAPI        { return etcd.NewKeysAPI(s.etcdClient) }
func (s *AllocRunSuite) PathRoot() string             { return s.pathRoot }
func (s *AllocRunSuite) Replicas() int                { return s.replicas }
func (s *AllocRunSuite) FixedItems() []string         { return s.fixedItems }
func (s *AllocRunSuite) ItemState(item string) string { return "ready" }

func (s *AllocRunSuite) ItemIsReadyForPromotion(item, state string) bool {
	return state == "ready"
}

func (s *AllocRunSuite) ItemRoute(item string, rt Route, ind int, tree *etcd.Node) {
	s.routesMu.Lock()
	defer s.routesMu.Unlock()

	s.routes[item] = rt.Copy()
}

// Represents a specific Allocator in the test context.
// Also implements testNotifier interface used by Allocate().
type testAllocator struct {
	*AllocRunSuite

	instanceKey string
	inspectCh   chan func(*etcd.Node)
}

// Allocator interface.
func (t testAllocator) InstanceKey() string                { return t.instanceKey }
func (t testAllocator) InspectChan() chan func(*etcd.Node) { return t.inspectCh }

// testNotifier interface.
func (t testAllocator) IdleAt(index uint64) {
	t.notifyCh <- notify{key: t.instanceKey, index: index}
}
func (t testAllocator) ActedAt(index uint64) {
	t.notifyCh <- notify{index: index}
}

func newTestAlloc(s *AllocRunSuite, key string) testAllocator {
	return testAllocator{
		AllocRunSuite: s,
		instanceKey:   key,
	}
}

func (t testAllocator) createAndRun(c *gc.C) {
	c.Assert(Create(t), gc.IsNil)
	c.Check(Allocate(t), gc.IsNil)
	t.notifyCh <- notify{key: t.instanceKey, exit: true}
}

func (t testAllocator) run(c *gc.C) {
	c.Check(Allocate(t), gc.IsNil)
	t.notifyCh <- notify{key: t.instanceKey, exit: true}
}

type waitFor struct{ idle, exit []string }

type notify struct {
	key   string
	index uint64
	exit  bool
}

var _ = gc.Suite(&AllocRunSuite{})
