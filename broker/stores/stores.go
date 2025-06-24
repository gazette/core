package stores

import (
	"context"
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	pb "go.gazette.dev/core/broker/protocol"
)

var (
	constructors = make(map[string]Constructor)
	stores       = make(map[pb.FragmentStore]*ActiveStore)
	storesMu     sync.RWMutex
)

// RegisterProviders registers store constructors for different storage schemes.
// This should be called during initialization to register all available store types.
func RegisterProviders(providers map[string]Constructor) {
	storesMu.Lock()
	defer storesMu.Unlock()

	for scheme, constructor := range providers {
		constructors[scheme] = constructor
	}
}

// GetProviders returns a copy of the currently registered store constructors.
// This is useful for tests that need to preserve and restore providers.
func GetProviders() map[string]Constructor {
	storesMu.RLock()
	defer storesMu.RUnlock()

	var copy = make(map[string]Constructor, len(constructors))
	for scheme, constructor := range constructors {
		copy[scheme] = constructor
	}
	return copy
}

// Get returns an ActiveStore for the given FragmentStore configuration.
// It will attempt to initialize the store if not already cached.
func Get(fs pb.FragmentStore) (*ActiveStore, error) {
	// Fast path: check if store already exists
	storesMu.RLock()
	if activeStore, ok := stores[fs]; ok {
		storesMu.RUnlock()
		return activeStore, nil
	}
	storesMu.RUnlock()

	// Slow path: need to initialize
	storesMu.Lock()
	defer storesMu.Unlock()

	// Double-check after acquiring write lock
	if activeStore, ok := stores[fs]; ok {
		return activeStore, nil
	}

	// Attempt to construct the store
	var ep = fs.URL()
	constructor, ok := constructors[ep.Scheme]
	if !ok {
		return nil, fmt.Errorf("unsupported fragment store scheme: %s", ep.Scheme)
	}

	store, err := constructor(ep)
	if err != nil {
		// Return error but don't cache - will retry on next call
		return nil, err
	}

	// Success - wrap in ActiveStore and cache
	var activeStore = &ActiveStore{
		Store: store,
		label: string(fs),
	}
	stores[fs] = activeStore

	// Update active stores metric
	activeStores.Set(float64(len(stores)))

	return activeStore, nil
}

// Sweep removes any stores that haven't been marked since the last sweep.
// Returns the number of stores removed.
func Sweep() int {
	storesMu.Lock()
	defer storesMu.Unlock()

	var removed int
	for fs, activeStore := range stores {
		if !activeStore.Mark.Load() {
			// Store hasn't been accessed since last sweep
			delete(stores, fs)
			removed++
		} else {
			// Clear mark for next sweep cycle
			activeStore.Mark.Store(false)
		}
	}

	// Update active stores metric
	activeStores.Set(float64(len(stores)))

	return removed
}

var (
	activeStores = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gazette_store_active",
		Help: "Number of active fragment stores",
	})

	storeOperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gazette_store_operation_duration_seconds",
		Help:    "Duration of store operations in seconds",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to ~32s
	}, []string{"store", "operation", "status"})

	storeOperationTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gazette_store_operation_total",
		Help: "Total number of store operations",
	}, []string{"store", "operation", "status"})

	storePutContentSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gazette_store_put_content_bytes",
		Help:    "Size of content written to stores in bytes",
		Buckets: prometheus.ExponentialBuckets(1024, 4, 10), // 1KB to ~1GB
	}, []string{"store", "encoding"})

	storeListItems = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gazette_store_list_items_count",
		Help:    "Number of items returned by list operations",
		Buckets: prometheus.ExponentialBuckets(1, 2, 15), // 1 to ~32k items
	}, []string{"store"})
)
