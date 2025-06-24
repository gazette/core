package stores

import (
	"context"
	"io"
	"sync/atomic"
	"time"
)

// ActiveStore wraps a Store implementation with mark-and-sweep GC support and instrumentation.
type ActiveStore struct {
	Store
	Mark atomic.Bool // Mark bit for GC

	label string // For metrics labels
}

// SignGet returns a pre-signed URL for GET operations with the given duration.
func (s *ActiveStore) SignGet(path string, d time.Duration) (string, error) {
	var started = time.Now()
	var signed, err = s.Store.SignGet(path, d)

	var status = "success"
	if err != nil {
		status = "error"
	}

	storeOperationTotal.WithLabelValues(s.label, "signget", status).Inc()
	storeOperationDuration.WithLabelValues(s.label, "signget", status).Observe(time.Since(started).Seconds())

	return signed, err
}

// Exists checks if content exists at the given path.
func (s *ActiveStore) Exists(ctx context.Context, path string) (bool, error) {
	var started = time.Now()
	var exists, err = s.Store.Exists(ctx, path)

	var status = "success"
	if err != nil {
		status = "error"
	}

	storeOperationTotal.WithLabelValues(s.label, "exists", status).Inc()
	storeOperationDuration.WithLabelValues(s.label, "exists", status).Observe(time.Since(started).Seconds())

	return exists, err
}

// Get returns an io.ReadCloser for content at the given path.
func (s *ActiveStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	var started = time.Now()
	var rc, err = s.Store.Get(ctx, path)

	var status = "success"
	if err != nil {
		status = "error"
	}

	storeOperationTotal.WithLabelValues(s.label, "get", status).Inc()
	storeOperationDuration.WithLabelValues(s.label, "get", status).Observe(time.Since(started).Seconds())

	return rc, err
}

// Put durably writes content to the store at the given path.
func (s *ActiveStore) Put(ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
	var started = time.Now()
	var err = s.Store.Put(ctx, path, content, contentLength, contentEncoding)

	var status = "success"
	if err != nil {
		status = "error"
	}

	storeOperationTotal.WithLabelValues(s.label, "put", status).Inc()
	storeOperationDuration.WithLabelValues(s.label, "put", status).Observe(time.Since(started).Seconds())

	// Track content size for successful puts
	if err == nil && contentLength > 0 {
		var encoding = contentEncoding
		if encoding == "" {
			encoding = "none"
		}
		storePutContentSize.WithLabelValues(s.label, encoding).Observe(float64(contentLength))
	}

	return err
}

// List enumerates all objects under the given prefix.
func (s *ActiveStore) List(ctx context.Context, prefix string, callback func(path string, modTime time.Time) error) error {
	var started = time.Now()

	// Wrap callback to count items
	var itemCount int64
	var wrappedCallback = func(path string, modTime time.Time) error {
		itemCount++
		return callback(path, modTime)
	}

	var err = s.Store.List(ctx, prefix, wrappedCallback)

	var status = "success"
	if err != nil {
		status = "error"
	}

	storeOperationTotal.WithLabelValues(s.label, "list", status).Inc()
	storeOperationDuration.WithLabelValues(s.label, "list", status).Observe(time.Since(started).Seconds())

	// Track item count for all list operations
	storeListItems.WithLabelValues(s.label).Observe(float64(itemCount))

	return err
}

// Remove deletes content at the given path.
func (s *ActiveStore) Remove(ctx context.Context, path string) error {
	var started = time.Now()
	var err = s.Store.Remove(ctx, path)

	var status = "success"
	if err != nil {
		status = "error"
	}

	storeOperationTotal.WithLabelValues(s.label, "remove", status).Inc()
	storeOperationDuration.WithLabelValues(s.label, "remove", status).Observe(time.Since(started).Seconds())

	return err
}
