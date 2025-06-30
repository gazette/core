package stores

import (
	"context"
	"io"
	"time"
)

// CallbackStore implements Store for testing with customizable behavior.
// It allows tests to provide callback functions for each Store method.
type CallbackStore struct {
	ProviderFunc    func() string
	SignGetFunc     func(path string, d time.Duration) (string, error)
	GetFunc         func(ctx context.Context, path string) (io.ReadCloser, error)
	ExistsFunc      func(ctx context.Context, path string) (bool, error)
	PutFunc         func(ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error
	ListFunc        func(ctx context.Context, prefix string, callback func(path string, modTime time.Time) error) error
	RemoveFunc      func(ctx context.Context, path string) error
	IsAuthErrorFunc func(error) bool
}

// Provider returns the provider name, or "callback" if ProviderFunc is nil.
func (c *CallbackStore) Provider() string {
	if c.ProviderFunc != nil {
		return c.ProviderFunc()
	}
	return "callback"
}

// SignGet calls SignGetFunc if set, otherwise returns an empty string.
func (c *CallbackStore) SignGet(path string, d time.Duration) (string, error) {
	if c.SignGetFunc != nil {
		return c.SignGetFunc(path, d)
	}
	return "", nil
}

// Exists calls ExistsFunc if set, otherwise returns false.
func (c *CallbackStore) Exists(ctx context.Context, path string) (bool, error) {
	if c.ExistsFunc != nil {
		return c.ExistsFunc(ctx, path)
	}
	return false, nil
}

// Get calls GetFunc if set, otherwise returns nil.
func (c *CallbackStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	if c.GetFunc != nil {
		return c.GetFunc(ctx, path)
	}
	return nil, nil
}

// Put calls PutFunc if set, otherwise returns nil.
func (c *CallbackStore) Put(ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
	if c.PutFunc != nil {
		return c.PutFunc(ctx, path, content, contentLength, contentEncoding)
	}
	return nil
}

// List calls ListFunc if set, otherwise returns nil.
func (c *CallbackStore) List(ctx context.Context, prefix string, callback func(path string, modTime time.Time) error) error {
	if c.ListFunc != nil {
		return c.ListFunc(ctx, prefix, callback)
	}
	return nil
}

// Remove calls RemoveFunc if set, otherwise returns nil.
func (c *CallbackStore) Remove(ctx context.Context, path string) error {
	if c.RemoveFunc != nil {
		return c.RemoveFunc(ctx, path)
	}
	return nil
}

// IsAuthError calls IsAuthErrorFunc if set, otherwise returns false.
func (c *CallbackStore) IsAuthError(err error) bool {
	if c.IsAuthErrorFunc != nil {
		return c.IsAuthErrorFunc(err)
	}
	return false
}
