package fragment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"
	"text/template"
	"time"

	pkgerrors "github.com/pkg/errors"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/stores"
	"go.gazette.dev/core/broker/stores/azure"
	"go.gazette.dev/core/broker/stores/fs"
	"go.gazette.dev/core/broker/stores/gcs"
	"go.gazette.dev/core/broker/stores/s3"
)

// ForceFileStore reinterprets journal stores to use the file:// scheme, ignoring
// a configured cloud scheme and bucket such as s3:// or gcs://
var ForceFileStore bool = false


// BoundStore embeds a Store instance and provides a place for additional
// metadata such as metrics and health status.
type BoundStore struct {
	stores.Store
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
		if spool.Fragment.CompressionCodec != pb.CompressionCodec_NONE {
			spool.finishCompression()
		}

		// We expect persisting individual spools to be fast, but have seen bugs
		// in the past where eg a storage backend behavior change caused occasional
		// multi-part upload failures such that the client wedged retrying
		// indefinitely. Use a generous timeout to detect and recover from this
		// class of error.
		var timeoutCtx, cancel = context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()

		err = s.Persist(timeoutCtx, &spool)
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
func constructStore(store pb.FragmentStore) (stores.Store, error) {
	var ep *url.URL
	if ForceFileStore {
		ep = pb.FragmentStore("file://").URL()
	} else {
		ep = store.URL()
	}

	var s stores.Store
	var err error

	switch ep.Scheme {
	case "s3":
		s, err = s3.New(ep)
	case "gs":
		s, err = gcs.New(ep)
	case "azure":
		s, err = azure.NewAccount(ep)
	case "azure-ad":
		s, err = azure.NewAD(ep)
	case "file":
		s, err = fs.New(ep)
	default:
		err = fmt.Errorf("unsupported scheme: %s", ep.Scheme)
	}

	return s, err
}

// RegisterStores updates the global BoundStore index with the provided set of stores.
// It takes ownership of the newStores map, which should have FragmentStore keys with nil values.
func RegisterStores(newStores map[pb.FragmentStore]*BoundStore) {
	storeIndexMu.Lock()
	defer storeIndexMu.Unlock()

	// Copy existing BoundStore values to new map, or construct new ones.
	for key := range newStores {
		if existing, ok := storeIndex[key]; ok && existing.initErr == nil {
			newStores[key] = existing
			delete(storeIndex, key)
		} else {
			var s, initErr = constructStore(key)
			newStores[key] = &BoundStore{Store: s, initErr: initErr}
		}
	}

	// `storeIndex` now holds only dropped FragmentStore keys.

	// Atomic swap
	storeIndex = newStores
}

func getStore(store pb.FragmentStore) (stores.Store, error) {
	storeIndexMu.Lock()
	defer storeIndexMu.Unlock()

	if bs, ok := storeIndex[store]; ok {
		return bs.Store, bs.initErr
	}
	return nil, fmt.Errorf("store not found: %s", store)
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


var (
	storeIndex   = make(map[pb.FragmentStore]*BoundStore)
	storeIndexMu sync.Mutex
)
