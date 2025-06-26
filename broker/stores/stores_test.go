package stores

import (
	"errors"
	"net/url"
	"sync"
	"testing"

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
	s1 := Get(pb.FragmentStore("file:///tmp/store1/"))
	require.NoError(t, s1.initErr)
	require.Equal(t, "/tmp/store1/", s1.Provider())

	// Get same store again - should return cached instance
	s2 := Get(pb.FragmentStore("file:///tmp/store1/"))
	require.NoError(t, s2.initErr)
	require.Same(t, s1, s2) // Same pointer

	// Get different store - should create new instance
	s3 := Get(pb.FragmentStore("file:///tmp/store2/"))
	require.NoError(t, s3.initErr)
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
	store := Get(pb.FragmentStore("file:///error/"))
	require.Error(t, store.initErr)
	require.Equal(t, "init failed", store.initErr.Error())

	// Verify store was not cached
	storesMu.RLock()
	_, exists := stores[pb.FragmentStore("file:///error/")]
	storesMu.RUnlock()
	require.False(t, exists)
}

func TestUnsupportedScheme(t *testing.T) {
	clearStores()

	// No constructors registered

	// Get store with unsupported scheme
	store := Get(pb.FragmentStore("s3://bucket/path/"))
	require.Error(t, store.initErr)
	require.Contains(t, store.initErr.Error(), "unsupported fragment store scheme: s3")
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
	store := Get(pb.FragmentStore("file:///tmp/test/"))
	require.Error(t, store.initErr)
	require.Equal(t, "temporary failure", store.initErr.Error())

	mu.Lock()
	require.Equal(t, 1, callCount)
	mu.Unlock()

	// Fix the constructor behavior
	mu.Lock()
	shouldFail = false
	mu.Unlock()

	// Second attempt - constructor should be called again and succeed
	s := Get(pb.FragmentStore("file:///tmp/test/"))
	require.NoError(t, s.initErr)
	require.NotNil(t, s)
	require.Equal(t, "/tmp/test/", s.Provider())

	mu.Lock()
	require.Equal(t, 2, callCount)
	mu.Unlock()

	// Third attempt - should return cached store without calling constructor
	s2 := Get(pb.FragmentStore("file:///tmp/test/"))
	require.NoError(t, s2.initErr)
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
			s := Get(pb.FragmentStore("file:///concurrent/"))

			mu.Lock()
			stores = append(stores, s)
			errors = append(errors, s.initErr)
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
	store1 := Get(pb.FragmentStore("file:///tmp/store1/"))
	require.NoError(t, store1.initErr)
	s2 := Get(pb.FragmentStore("file:///tmp/store2/"))
	require.NoError(t, s2.initErr)
	store3 := Get(pb.FragmentStore("file:///tmp/store3/"))
	require.NoError(t, store3.initErr)

	// Verify all stores exist
	storesMu.RLock()
	require.Equal(t, 3, len(stores))
	storesMu.RUnlock()

	// Mark all stores as in-use
	for _, fs := range []pb.FragmentStore{"file:///tmp/store1/", "file:///tmp/store2/", "file:///tmp/store3/"} {
		store := Get(fs)
		require.NoError(t, store.initErr)
		store.Mark.Store(true)
	}

	// First sweep should not remove any stores (all were just accessed)
	var removed = Sweep()
	require.Equal(t, 0, removed)

	// Mark only store1 and store3 as in-use
	store1 = Get(pb.FragmentStore("file:///tmp/store1/"))
	require.NoError(t, store1.initErr)
	store1.Mark.Store(true)
	store3 = Get(pb.FragmentStore("file:///tmp/store3/"))
	require.NoError(t, store3.initErr)
	store3.Mark.Store(true)

	// Second sweep should remove store2 (not marked)
	removed = Sweep()
	require.Equal(t, 1, removed)

	// Verify only two stores remain
	storesMu.RLock()
	require.Equal(t, 2, len(stores))
	_, exists := stores[pb.FragmentStore("file:///tmp/store2/")]
	require.False(t, exists)
	storesMu.RUnlock()

	// Verify health status of removed store returns error
	store2 := Get(pb.FragmentStore("file:///tmp/store2/"))
	require.NoError(t, store2.initErr) // New instance created

	// Verify it's a new instance
	require.NotSame(t, s2, store2)
}

func TestSweepWithInitErrors(t *testing.T) {
	clearStores()

	// Register providers that succeed and fail
	var successConstructor = func(u *url.URL) (Store, error) {
		return &CallbackStore{ProviderFunc: func() string { return u.Path }}, nil
	}
	var failConstructor = func(u *url.URL) (Store, error) {
		return nil, errors.New("init failed")
	}

	RegisterProviders(map[string]Constructor{
		"file": successConstructor,
		"s3":   failConstructor,
	})

	// Create one successful store
	store1 := Get(pb.FragmentStore("file:///tmp/store1/"))
	require.NoError(t, store1.initErr)

	// Create stores that fail to initialize
	store2 := Get(pb.FragmentStore("s3://bucket/tmp/store2/"))
	require.Error(t, store2.initErr)
	store3 := Get(pb.FragmentStore("s3://bucket/tmp/store3/"))
	require.Error(t, store3.initErr)

	// Verify only the successful store is cached
	storesMu.RLock()
	require.Equal(t, 1, len(stores))
	_, exists := stores[pb.FragmentStore("file:///tmp/store1/")]
	require.True(t, exists)
	storesMu.RUnlock()

	// Sweep should not affect non-cached stores
	var removed = Sweep()
	require.Equal(t, 1, removed) // The one cached store (unmarked)

	// Verify all stores were removed (none were marked)
	storesMu.RLock()
	require.Equal(t, 0, len(stores))
	storesMu.RUnlock()
}

func TestGetProviders(t *testing.T) {
	clearStores()

	var providers = map[string]Constructor{
		"file": func(u *url.URL) (Store, error) {
			return &CallbackStore{ProviderFunc: func() string { return "file" }}, nil
		},
		"s3": func(u *url.URL) (Store, error) {
			return &CallbackStore{ProviderFunc: func() string { return "s3" }}, nil
		},
	}

	RegisterProviders(providers)

	// Get providers should return a copy
	gotProviders := GetProviders()
	require.Equal(t, 2, len(gotProviders))
	require.NotNil(t, gotProviders["file"])
	require.NotNil(t, gotProviders["s3"])

	// Verify it's a copy by modifying it
	delete(gotProviders, "file")
	require.Equal(t, 1, len(gotProviders))

	// Original should be unchanged
	storesMu.RLock()
	require.Equal(t, 2, len(constructors))
	storesMu.RUnlock()
}

func TestNewActiveStore(t *testing.T) {
	// Test creating an ActiveStore directly
	var mockStore = &CallbackStore{
		ProviderFunc: func() string { return "mock" },
	}

	// Test with no init error
	store := NewActiveStore(pb.FragmentStore("file:///test/"), mockStore, nil)
	require.NoError(t, store.initErr)
	require.Equal(t, pb.FragmentStore("file:///test/"), store.Key)
	require.Same(t, mockStore, store.Store)

	// Check initial health status
	var done, err = store.HealthStatus()
	require.NotNil(t, done)
	require.Error(t, err)
	require.Contains(t, err.Error(), "first health check hasn't completed yet")

	// Test with init error
	var initErr = errors.New("initialization failed")
	store2 := NewActiveStore(pb.FragmentStore("file:///error/"), mockStore, initErr)
	require.Error(t, store2.initErr)
	require.Equal(t, initErr, store2.initErr)

	// Check health status returns init error
	done, err = store2.HealthStatus()
	require.NotNil(t, done)
	require.Equal(t, initErr, err)

	// Done channel should already be closed for error case
	select {
	case <-done:
		// Good, it's closed
	default:
		t.Fatal("done channel should be closed for stores with init error")
	}
}

func TestUpdateHealth(t *testing.T) {
	var mockStore = &CallbackStore{
		ProviderFunc: func() string { return "mock" },
	}

	store := NewActiveStore(pb.FragmentStore("file:///test/"), mockStore, nil)

	// Get initial status
	var done1, err1 = store.HealthStatus()
	require.Error(t, err1)

	// Update health to success
	store.UpdateHealth(nil)

	// Original done channel should be closed
	select {
	case <-done1:
		// Good
	default:
		t.Fatal("done channel should be closed after health update")
	}

	// Get new status
	var done2, err2 = store.HealthStatus()
	require.NoError(t, err2)
	require.NotEqual(t, done1, done2) // Should be a new channel

	// Update health to error
	var healthErr = errors.New("health check failed")
	store.UpdateHealth(healthErr)

	// Done2 should be closed
	select {
	case <-done2:
		// Good
	default:
		t.Fatal("done channel should be closed after health update")
	}

	// Get error status
	var _, err3 = store.HealthStatus()
	require.Equal(t, healthErr, err3)
}

func TestMarkAndSweepBehavior(t *testing.T) {
	clearStores()

	var constructors = map[string]Constructor{
		"file": func(u *url.URL) (Store, error) {
			return &CallbackStore{ProviderFunc: func() string { return u.Path }}, nil
		},
	}

	RegisterProviders(constructors)

	// Create a store
	store := Get(pb.FragmentStore("file:///test/"))
	require.NoError(t, store.initErr)

	// Initial mark should be false
	require.False(t, store.Mark.Load())

	// Set mark
	store.Mark.Store(true)
	require.True(t, store.Mark.Load())

	// Sweep should not remove marked store
	var removed = Sweep()
	require.Equal(t, 0, removed)

	// Mark should be cleared after sweep
	require.False(t, store.Mark.Load())

	// Another sweep should remove unmarked store
	removed = Sweep()
	require.Equal(t, 1, removed)

	// Get the store again - should be a new instance
	store2 := Get(pb.FragmentStore("file:///test/"))
	require.NoError(t, store2.initErr)
	require.NotSame(t, store, store2)
}

func TestHealthStatusAfterSweep(t *testing.T) {
	clearStores()

	var constructors = map[string]Constructor{
		"file": func(u *url.URL) (Store, error) {
			return &CallbackStore{ProviderFunc: func() string { return u.Path }}, nil
		},
	}

	RegisterProviders(constructors)

	// Create a store
	store := Get(pb.FragmentStore("file:///test/"))
	require.NoError(t, store.initErr)

	// Update health to success
	store.UpdateHealth(nil)

	// Verify health is good
	var _, err = store.HealthStatus()
	require.NoError(t, err)

	// Remove the store via sweep
	var removed = Sweep()
	require.Equal(t, 1, removed)

	// Health status should now indicate stopped checks
	var done, err2 = store.HealthStatus()
	require.Error(t, err2)
	require.Contains(t, err2.Error(), "health checks have stopped")

	// Done channel should be closed
	select {
	case <-done:
		// Good
	default:
		t.Fatal("done channel should be closed for swept stores")
	}
}
