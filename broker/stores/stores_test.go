package stores

import (
	"errors"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
)

// clearStores is a test helper to reset the global state
func clearStores() {
	storesMu.Lock()
	defer storesMu.Unlock()
	constructors = make(map[string]Constructor)
	stores = make(map[pb.FragmentStore]*ActiveStore)
}

func TestRegisterProviders(t *testing.T) {
	clearStores()

	var providers = map[string]Constructor{
		"mock": func(u *url.URL) (Store, error) {
			return &CallbackStore{ProviderFunc: func() string { return "mock" }}, nil
		},
		"error": func(u *url.URL) (Store, error) {
			return nil, errors.New("constructor error")
		},
	}

	RegisterProviders(providers)

	// Verify constructors were registered
	storesMu.RLock()
	require.Equal(t, 2, len(constructors))
	require.NotNil(t, constructors["mock"])
	require.NotNil(t, constructors["error"])
	storesMu.RUnlock()
}

func TestGetStore(t *testing.T) {
	clearStores()

	var constructors = map[string]Constructor{
		"file": func(u *url.URL) (Store, error) {
			// Use path component for provider identification in tests
			return &CallbackStore{ProviderFunc: func() string { return u.Path }}, nil
		},
	}

	RegisterProviders(constructors)

	// Get store for the first time - should create it
	s1, err := Get("file:///tmp/store1/")
	require.NoError(t, err)
	require.Equal(t, "/tmp/store1/", s1.Provider())

	// Get same store again - should return cached instance
	s2, err := Get("file:///tmp/store1/")
	require.NoError(t, err)
	require.Same(t, s1, s2) // Same pointer

	// Get different store - should create new instance
	s3, err := Get("file:///tmp/store2/")
	require.NoError(t, err)
	require.Equal(t, "/tmp/store2/", s3.Provider())
	require.NotSame(t, s1, s3)
}

func TestConstructorErrors(t *testing.T) {
	clearStores()

	var constructors = map[string]Constructor{
		"file": func(u *url.URL) (Store, error) {
			return nil, errors.New("init failed")
		},
	}

	RegisterProviders(constructors)

	// Get store that will fail construction
	_, err := Get("file:///error/")
	require.Error(t, err)
	require.Equal(t, "init failed", err.Error())

	// Verify store was not cached
	storesMu.RLock()
	_, exists := stores["file:///error/"]
	storesMu.RUnlock()
	require.False(t, exists)
}

func TestUnsupportedScheme(t *testing.T) {
	clearStores()

	// No constructors registered

	// Get store with unsupported scheme
	_, err := Get("s3://bucket/path/")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported fragment store scheme: s3")
}

func TestRetryAfterInitError(t *testing.T) {
	clearStores()

	// Track constructor calls
	var callCount = 0
	var shouldFail = true
	var mu sync.Mutex

	var constructors = map[string]Constructor{
		"file": func(u *url.URL) (Store, error) {
			mu.Lock()
			defer mu.Unlock()
			callCount++
			if shouldFail {
				return nil, errors.New("temporary failure")
			}
			return &CallbackStore{ProviderFunc: func() string { return u.Path }}, nil
		},
	}

	RegisterProviders(constructors)

	// First attempt - constructor should fail
	_, err := Get("file:///tmp/test/")
	require.Error(t, err)
	require.Equal(t, "temporary failure", err.Error())

	mu.Lock()
	require.Equal(t, 1, callCount)
	mu.Unlock()

	// Fix the constructor behavior
	mu.Lock()
	shouldFail = false
	mu.Unlock()

	// Second attempt - constructor should be called again and succeed
	s, err := Get("file:///tmp/test/")
	require.NoError(t, err)
	require.NotNil(t, s)
	require.Equal(t, "/tmp/test/", s.Provider())

	mu.Lock()
	require.Equal(t, 2, callCount)
	mu.Unlock()

	// Third attempt - should return cached store without calling constructor
	s2, err := Get("file:///tmp/test/")
	require.NoError(t, err)
	require.Same(t, s, s2) // Same pointer

	mu.Lock()
	require.Equal(t, 2, callCount) // Constructor not called again
	mu.Unlock()
}

func TestConcurrentAccess(t *testing.T) {
	clearStores()

	var constructors = map[string]Constructor{
		"file": func(u *url.URL) (Store, error) {
			// Simulate some work
			return &CallbackStore{ProviderFunc: func() string { return u.Path }}, nil
		},
	}

	RegisterProviders(constructors)

	// Test concurrent access to the same store
	var wg sync.WaitGroup
	const goroutines = 10
	wg.Add(goroutines)

	var stores []*ActiveStore
	var errors []error
	var mu sync.Mutex

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			s, err := Get("file:///concurrent/")

			mu.Lock()
			stores = append(stores, s)
			errors = append(errors, err)
			mu.Unlock()
		}()
	}

	wg.Wait()

	// All goroutines should get the same store instance
	require.Equal(t, goroutines, len(stores))
	require.Equal(t, goroutines, len(errors))

	for i := 0; i < goroutines; i++ {
		require.NoError(t, errors[i])
		require.Same(t, stores[0], stores[i]) // All should be same pointer
	}
}

func TestSweepRemovesUnmarkedStores(t *testing.T) {
	clearStores()

	var constructors = map[string]Constructor{
		"file": func(u *url.URL) (Store, error) {
			return &CallbackStore{ProviderFunc: func() string { return u.Path }}, nil
		},
	}

	RegisterProviders(constructors)

	// Create three stores
	_, err := Get("file:///tmp/store1/")
	require.NoError(t, err)
	s2, err := Get("file:///tmp/store2/")
	require.NoError(t, err)
	_, err = Get("file:///tmp/store3/")
	require.NoError(t, err)

	// Verify all stores exist
	storesMu.RLock()
	require.Equal(t, 3, len(stores))
	storesMu.RUnlock()

	// Mark all stores as in-use
	for _, fs := range []pb.FragmentStore{"file:///tmp/store1/", "file:///tmp/store2/", "file:///tmp/store3/"} {
		store, err := Get(fs)
		require.NoError(t, err)
		store.Mark.Store(true)
	}

	// First sweep should not remove any stores (all were just accessed)
	var removed = Sweep()
	require.Equal(t, 0, removed)

	// Mark only store1 and store3 as in-use
	store1, err := Get("file:///tmp/store1/")
	require.NoError(t, err)
	store1.Mark.Store(true)

	store3, err := Get("file:///tmp/store3/")
	require.NoError(t, err)
	store3.Mark.Store(true)

	// Second sweep should remove s2
	removed = Sweep()
	require.Equal(t, 1, removed)

	// Verify s2 was removed
	storesMu.RLock()
	require.Equal(t, 2, len(stores))
	_, exists := stores["file:///tmp/store2/"]
	require.False(t, exists)
	storesMu.RUnlock()

	// Getting s2 again should create a new instance
	s2New, err := Get("file:///tmp/store2/")
	require.NoError(t, err)
	require.NotSame(t, s2, s2New)
}

func TestSweepClearsMarkBits(t *testing.T) {
	clearStores()

	var constructors = map[string]Constructor{
		"file": func(u *url.URL) (Store, error) {
			return &CallbackStore{ProviderFunc: func() string { return u.Path }}, nil
		},
	}

	RegisterProviders(constructors)

	// Create a store and mark it as used
	store, err := Get("file:///tmp/store/")
	require.NoError(t, err)
	store.Mark.Store(true)

	// First sweep shouldn't remove it (marked as used)
	var removed = Sweep()
	require.Equal(t, 0, removed)

	// Second sweep should remove it (not accessed since last sweep)
	removed = Sweep()
	require.Equal(t, 1, removed)

	// Verify store was removed
	storesMu.RLock()
	require.Equal(t, 0, len(stores))
	storesMu.RUnlock()
}

func TestConcurrentSweepAndGet(t *testing.T) {
	clearStores()

	var constructors = map[string]Constructor{
		"file": func(u *url.URL) (Store, error) {
			return &CallbackStore{ProviderFunc: func() string { return u.Path }}, nil
		},
	}

	RegisterProviders(constructors)

	// Create some stores
	for i := 0; i < 10; i++ {
		_, err := Get(pb.FragmentStore(fmt.Sprintf("file:///tmp/store%d/", i)))
		require.NoError(t, err)
	}

	// Run concurrent sweeps and gets
	var wg sync.WaitGroup
	var stop = make(chan bool)
	var sweepCount atomic.Int32

	// Sweeper goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				Sweep()
				sweepCount.Add(1)
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	// Getter goroutines - continuously access stores 0-4
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					store, err := Get(pb.FragmentStore(fmt.Sprintf("file:///tmp/store%d/", id)))
					if err != nil {
						t.Errorf("Get failed: %v", err)
					} else {
						// Mark store as in-use
						store.Mark.Store(true)
					}
					time.Sleep(2 * time.Millisecond)
				}
			}
		}(i)
	}

	// Let it run for a bit
	time.Sleep(100 * time.Millisecond)

	// Mark stores 0-4 one more time before checking
	for i := 0; i < 5; i++ {
		store, err := Get(pb.FragmentStore(fmt.Sprintf("file:///tmp/store%d/", i)))
		require.NoError(t, err)
		store.Mark.Store(true)
	}

	close(stop)
	wg.Wait()

	// Ensure at least some sweeps occurred
	require.Greater(t, sweepCount.Load(), int32(5))

	// After one more sweep, stores 0-4 should still exist (just accessed)
	// but stores 5-9 should be gone
	Sweep()

	storesMu.RLock()
	for i := 0; i < 5; i++ {
		_, exists := stores[pb.FragmentStore(fmt.Sprintf("file:///tmp/store%d/", i))]
		require.True(t, exists, "Store %d should exist", i)
	}
	for i := 5; i < 10; i++ {
		_, exists := stores[pb.FragmentStore(fmt.Sprintf("file:///tmp/store%d/", i))]
		require.False(t, exists, "Store %d should have been swept", i)
	}
	storesMu.RUnlock()
}
