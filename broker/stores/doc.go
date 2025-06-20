// Package stores provides an abstraction over cloud storage systems for journal fragments.
// It defines the Store interface that all storage backends must implement, along with
// common configuration types used across providers.
package stores

import (
	"context"
	"io"
	"time"

	pb "go.gazette.dev/core/broker/protocol"
)

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

// File is the subset of os.File used in backing Fragments with local files.
type File interface {
	io.ReaderAt
	io.Seeker
	io.WriterAt
	io.Writer
	io.Closer
}

// Spool interface defines the methods required by Store.Persist.
// This avoids circular dependencies with the fragment package.
type Spool interface {
	GetFragment() *pb.Fragment
	File() File
	CompressedFile() File
	CompressedLength() int64
}