package fragment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/gorilla/schema"
	pkgerrors "github.com/pkg/errors"
	pb "go.gazette.dev/core/broker/protocol"
)

// ForceFileStore reinterprets journal stores to use the file:// scheme, ignoring
// a configured cloud scheme and bucket such as s3:// or gcs://
var ForceFileStore bool = false

// Store provides an abstraction over cloud storage systems for journal fragments.
type Store interface {
	// Provider returns the name of the storage backend (e.g., "s3", "gcs", "azure", "fs").
	Provider() string

	// SignGet returns a pre-signed URL that authenticates the bearer to perform
	// a GET operation of the Fragment for the provided Duration from the current time.
	// This allows clients to directly fetch fragments from the storage backend.
	SignGet(fragment pb.Fragment, d time.Duration) (string, error)

	// Exists checks if the given Fragment is present in the store.
	// Returns true if the fragment exists, false otherwise.
	Exists(ctx context.Context, fragment pb.Fragment) (bool, error)

	// Open returns an io.ReadCloser of the Fragment's content from the store.
	// The caller is responsible for closing the returned ReadCloser.
	// The returned reader does not perform client-side decompression, but may
	// request server-side decompression for GZIP_OFFLOAD_DECOMPRESSION.
	Open(ctx context.Context, fragment pb.Fragment) (io.ReadCloser, error)

	// Persist durably writes a Spool to the store.
	// If the Spool has not been compressed incrementally, it will be compressed
	// before persistence according to the Spool's CompressionCodec.
	Persist(ctx context.Context, spool Spool) error

	// List enumerates all Fragments of the given Journal in the store.
	// The callback is invoked for each Fragment found, with listing terminated
	// early if the callback returns an error.
	List(ctx context.Context, name pb.Journal, callback func(pb.Fragment)) error

	// Remove deletes the Fragment from the store.
	Remove(ctx context.Context, fragment pb.Fragment) error

	// IsAuthError returns true if the error represents an authorization failure
	// (e.g., missing permissions, bucket not found, access denied).
	// This is used to distinguish authorization errors from authentication or
	// other transient errors, particularly during graceful shutdown to allow
	// fallback to alternate stores.
	IsAuthError(error) bool
}

// BoundStore embeds a Store instance and provides a place for additional
// metadata such as metrics and health status.
type BoundStore struct {
	Store
	initErr error // Initialization error, if any
	// TODO: Add metrics and health status fields here
}

// SignGetURL returns a URL authenticating the bearer to perform a GET operation
// of the Fragment for the provided Duration from the current time.
func SignGetURL(fragment pb.Fragment, d time.Duration) (string, error) {
	var s, err = getStore(fragment.BackingStore)
	if err != nil {
		return "", err
	}

	var signedURL string
	signedURL, err = s.SignGet(fragment, d)
	instrumentStoreOp(s.Provider(), "get_signed_url", err)
	return signedURL, err
}

// Open a Reader of the Fragment on the store. The returned ReadCloser does not
// perform any applicable client-side decompression, but does request server
// decompression in the case of GZIP_OFFLOAD_DECOMPRESSION.
func Open(ctx context.Context, fragment pb.Fragment) (io.ReadCloser, error) {
	var s, err = getStore(fragment.BackingStore)
	if err != nil {
		return nil, err
	}

	rc, err := s.Open(ctx, fragment)
	instrumentStoreOp(s.Provider(), "open", err)
	return rc, err
}

// Persist a Spool to the JournalSpec's store. If the Spool Fragment is already
// present, this is a no-op. If the Spool has not been compressed incrementally,
// it will be compressed before being persisted. If `isExiting` and the preferred
// store returns an AuthZ error, it will try subsequent stores.
func Persist(ctx context.Context, spool Spool, spec *pb.JournalSpec, isExiting bool) error {
	var stores = spec.Fragment.Stores

	for len(stores) != 0 {
		spool.Fragment.BackingStore, stores = stores[0], stores[1:]

		if postfix, err := evalPathPostfix(spool, spec); err != nil {
			return err
		} else {
			spool.PathPostfix = postfix
		}

		var s, err = getStore(spool.Fragment.BackingStore)
		if err != nil {
			return err
		}

		exists, err := s.Exists(ctx, spool.Fragment.Fragment)
		instrumentStoreOp(s.Provider(), "exist", err)

		if err != nil {
			if s.IsAuthError(err) && isExiting && len(stores) != 0 {
				continue // Fall back to the next store.
			}
			return err
		} else if exists {
			return nil // All done.
		}

		// Ensure `compressedFile` is ready. This is a no-op if compressed incrementally.
		if spool.CompressionCodec != pb.CompressionCodec_NONE {
			spool.finishCompression()
		}

		// We expect persisting individual spools to be fast, but have seen bugs
		// in the past where eg a storage backend behavior change caused occasional
		// multi-part upload failures such that the client wedged retrying
		// indefinitely. Use a generous timeout to detect and recover from this
		// class of error.
		var timeoutCtx, cancel = context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()

		err = s.Persist(timeoutCtx, spool)
		instrumentStoreOp(s.Provider(), "persist", err)

		if err != nil {
			if s.IsAuthError(err) && isExiting && len(stores) != 0 {
				continue // During shutdown with auth errors, try next store.
			}
			return err
		}

		storePersistedBytesTotal.WithLabelValues(s.Provider()).Add(float64(spool.ContentLength()))
		return nil // Success
	}
	return nil // No stores.
}

// List Fragments of the FragmentStore for a given journal. |callback| is
// invoked with each listed Fragment, and any returned error aborts the listing.
func List(ctx context.Context, store pb.FragmentStore, name pb.Journal, callback func(pb.Fragment)) error {
	var s, err = getStore(store)
	if err != nil {
		return err
	}

	err = s.List(ctx, name, func(f pb.Fragment) { f.BackingStore = store; callback(f) })
	instrumentStoreOp(s.Provider(), "list", err)
	return err
}

// Remove |fragment| from its BackingStore.
func Remove(ctx context.Context, fragment pb.Fragment) error {
	var s, err = getStore(fragment.BackingStore)
	if err != nil {
		return err
	}
	err = s.Remove(ctx, fragment)
	instrumentStoreOp(s.Provider(), "remove", err)
	return err
}

// constructStore creates a new BoundStore instance for the provided FragmentStore.
// Any initialization errors are stored in the returned BoundStore.
func constructStore(store pb.FragmentStore) (Store, error) {
	var ep *url.URL
	if ForceFileStore {
		ep = pb.FragmentStore("file://").URL()
	} else {
		ep = store.URL()
	}

	var s Store
	var err error

	switch ep.Scheme {
	case "s3":
		s, err = newS3Store(ep)
	case "gs":
		s, err = newGCSStore(ep)
	case "azure":
		s, err = newAzureAccountStore(ep)
	case "azure-ad":
		s, err = newAzureADStore(ep)
	case "file":
		s, err = newFSStore(ep)
	default:
		err = fmt.Errorf("unsupported scheme: %s", ep.Scheme)
	}

	return s, err
}

// RegisterStores updates the global BoundStore index with the provided set of stores.
// It takes ownership of the newStores map, which should have FragmentStore keys with nil values.
func RegisterStores(newStores map[pb.FragmentStore]*BoundStore) {
	storesMu.Lock()
	defer storesMu.Unlock()

	// Copy existing BoundStore values to new map, or construct new ones.
	for key := range newStores {
		if existing, ok := stores[key]; ok && existing.initErr == nil {
			newStores[key] = existing
			delete(stores, key)
		} else {
			var s, initErr = constructStore(key)
			newStores[key] = &BoundStore{Store: s, initErr: initErr}
		}
	}

	// `stores` now holds only dropped FragmentStore keys.

	// Atomic swap
	stores = newStores
}

func getStore(store pb.FragmentStore) (Store, error) {
	storesMu.Lock()
	defer storesMu.Unlock()

	if bs, ok := stores[store]; ok {
		return bs.Store, bs.initErr
	}
	return nil, fmt.Errorf("store not found: %s", store)
}

func parseStoreArgs(ep *url.URL, args interface{}) error {
	var decoder = schema.NewDecoder()
	decoder.IgnoreUnknownKeys(false)

	if q, err := url.ParseQuery(ep.RawQuery); err != nil {
		return err
	} else if err = decoder.Decode(args, q); err != nil {
		return fmt.Errorf("parsing store URL arguments: %s", err)
	}
	return nil
}

func instrumentStoreOp(provider, op string, err error) {
	var cause = errors.Unwrap(err)
	if err != nil {
		if cause != nil {
			storeRequestTotal.WithLabelValues(provider, op, cause.Error()).Inc()
		} else {
			storeRequestTotal.WithLabelValues(provider, op, err.Error()).Inc()
		}
	} else {
		storeRequestTotal.WithLabelValues(provider, op, "ok").Inc()
	}
}

func evalPathPostfix(spool Spool, spec *pb.JournalSpec) (string, error) {
	var tpl, err = template.New("").Parse(spec.Fragment.PathPostfixTemplate)
	if err != nil {
		return "", pkgerrors.WithMessage(err, "parsing PathPostfixTemplate")
	}

	var b bytes.Buffer
	if err = tpl.Execute(&b, struct {
		Spool
		*pb.JournalSpec
	}{spool, spec}); err != nil {
		return "", pkgerrors.WithMessagef(err,
			"executing PathPostfixTemplate (%s)", spec.Fragment.PathPostfixTemplate)
	}
	return b.String(), nil
}

// RewriterConfig rewrites the path under which journal fragments are stored
// by finding and replacing a portion of the journal's name in the final
// constructed path. Its use is uncommon and not recommended, but it can help
// in the implementation of new journal naming taxonomies which don't disrupt
// journal fragments that are already written.
//
//	var cfg = RewriterConfig{
//	    Replace: "/old-path/page-views/
//	    Find:    "/bar/v1/page-views/",
//	}
//	// Remaps journal name => fragment store URL:
//	//  "/foo/bar/v1/page-views/part-000" => "s3://my-bucket/foo/old-path/page-views/part-000" // Matched.
//	//  "/foo/bar/v2/page-views/part-000" => "s3://my-bucket/foo/bar/v2/page-views/part-000"   // Not matched.
type RewriterConfig struct {
	// Find is the string to replace in the unmodified journal name.
	Find string
	// Replace is the string with which Find is replaced in the constructed store path.
	Replace string
}

// rewritePath replace the first occurrence of the find string with the replace
// string in journal path |j| and appends it to the fragment store path |s|,
// effectively rewriting the default Journal path. If find is empty or not
// found, the original |j| is appended.
func (cfg RewriterConfig) rewritePath(s, j string) string {
	if cfg.Find == "" {
		return s + j
	}
	return s + strings.Replace(j, cfg.Find, cfg.Replace, 1)
}

var (
	stores   = make(map[pb.FragmentStore]*BoundStore)
	storesMu sync.Mutex

	httpClientDisableCompression = &http.Client{
		Transport: &http.Transport{DisableCompression: true},
	}
)
