package stores

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
)

// errorReader is a reader that always returns an error
type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, errors.New("read error")
}

func TestHealthChecks(t *testing.T) {
	const testContent = "health-check\n"

	tests := []struct {
		name          string
		expectedError string
		setupStore    func(*httptest.Server) Constructor
	}{
		{
			name: "Success",
			setupStore: func(server *httptest.Server) Constructor {
				return func(u *url.URL) (Store, error) {
					return &CallbackStore{
						ProviderFunc: func() string { return "file" },
						PutFunc: func(ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
							// Verify correct path and content
							require.Equal(t, ".test/health-check", path)
							require.Equal(t, int64(len(testContent)), contentLength)

							var buf = make([]byte, contentLength)
							_, err := content.ReadAt(buf, 0)
							require.NoError(t, err)
							require.Equal(t, testContent, string(buf))
							return nil
						},
						GetFunc: func(ctx context.Context, path string) (io.ReadCloser, error) {
							require.Equal(t, ".test/health-check", path)
							return io.NopCloser(strings.NewReader(testContent)), nil
						},
						ListFunc: func(ctx context.Context, prefix string, callback func(path string, modTime time.Time) error) error {
							require.Equal(t, ".test/", prefix)
							return callback("health-check", time.Now())
						},
						SignGetFunc: func(path string, d time.Duration) (string, error) {
							require.Equal(t, ".test/health-check", path)
							return server.URL + "/signed/test", nil
						},
					}, nil
				}
			},
		},
		{
			name:          "GET Failure",
			expectedError: "health check GET failed",
			setupStore: func(server *httptest.Server) Constructor {
				return func(u *url.URL) (Store, error) {
					return &CallbackStore{
						ProviderFunc: func() string { return "file" },
						PutFunc: func(ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
							return nil
						},
						GetFunc: func(ctx context.Context, path string) (io.ReadCloser, error) {
							return nil, errors.New("simulated GET failure")
						},
					}, nil
				}
			},
		},
		{
			name:          "Content Mismatch",
			expectedError: "health check content mismatch",
			setupStore: func(server *httptest.Server) Constructor {
				return func(u *url.URL) (Store, error) {
					return &CallbackStore{
						ProviderFunc: func() string { return "file" },
						PutFunc: func(ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
							return nil
						},
						GetFunc: func(ctx context.Context, path string) (io.ReadCloser, error) {
							return io.NopCloser(strings.NewReader("wrong content")), nil
						},
						ListFunc: func(ctx context.Context, prefix string, callback func(path string, modTime time.Time) error) error {
							return callback("health-check", time.Now())
						},
						SignGetFunc: func(path string, d time.Duration) (string, error) {
							return server.URL + "/wrong", nil
						},
					}, nil
				}
			},
		},
		{
			name:          "GET Returns Nil Reader",
			expectedError: "health check GET returned nil reader",
			setupStore: func(server *httptest.Server) Constructor {
				return func(u *url.URL) (Store, error) {
					return &CallbackStore{
						ProviderFunc: func() string { return "file" },
						PutFunc: func(ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
							return nil
						},
						GetFunc: func(ctx context.Context, path string) (io.ReadCloser, error) {
							return nil, nil // Return nil reader without error
						},
					}, nil
				}
			},
		},
		{
			name:          "Read Failure",
			expectedError: "health check read failed",
			setupStore: func(server *httptest.Server) Constructor {
				return func(u *url.URL) (Store, error) {
					return &CallbackStore{
						ProviderFunc: func() string { return "file" },
						PutFunc: func(ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
							return nil
						},
						GetFunc: func(ctx context.Context, path string) (io.ReadCloser, error) {
							// Return a reader that fails on Read
							return io.NopCloser(errorReader{}), nil
						},
					}, nil
				}
			},
		},
		{
			name:          "Failure Then Success",
			expectedError: "health check GET failed", // Expect failure on first attempt
			setupStore: func(server *httptest.Server) Constructor {
				return func(u *url.URL) (Store, error) {
					var attempt int
					return &CallbackStore{
						ProviderFunc: func() string { return "file" },
						PutFunc: func(ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
							return nil
						},
						GetFunc: func(ctx context.Context, path string) (io.ReadCloser, error) {
							attempt++
							if attempt == 1 {
								return nil, errors.New("simulated transient failure")
							}
							return io.NopCloser(strings.NewReader(testContent)), nil
						},
						ListFunc: func(ctx context.Context, prefix string, callback func(path string, modTime time.Time) error) error {
							return callback("health-check", time.Now())
						},
						SignGetFunc: func(path string, d time.Duration) (string, error) {
							return server.URL + "/signed/test", nil
						},
					}, nil
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearStores()

			// Create test HTTP server
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/signed/test" {
					w.Write([]byte(testContent))
				} else {
					w.Write([]byte("wrong content"))
				}
			}))
			defer server.Close()

			RegisterProviders(map[string]Constructor{
				"file": tt.setupStore(server),
			})

			// Get store - this starts health check
			store := Get(pb.FragmentStore("file:///test/"))
			require.NoError(t, store.initErr)

			// Wait for first health check if needed.
			done, err := store.HealthStatus()
			if err == ErrFirstHealthCheck {
				<-done
				done, err = store.HealthStatus()
			}

			// Check result of first health check
			if tt.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
			}

			// For "Failure Then Success", wait for the retry to succeed
			if tt.name == "Failure Then Success" {
				<-done // Wait for next health check.
				_, err = store.HealthStatus()
				require.NoError(t, err)
			}
		})
	}
}
