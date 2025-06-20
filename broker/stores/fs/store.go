package fs

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/gorilla/schema"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/stores"
	"go.gazette.dev/core/broker/stores/common"
)

// FileSystemStoreRoot is the filesystem path which roots fragment ContentPaths
// of a file:// fragment store. It must be set at program startup prior to use.
var FileSystemStoreRoot = "/dev/null/must/configure/file/store/root"

// StoreQueryArgs contains fields that are parsed from the query arguments
// of a file:// fragment store URL.
type StoreQueryArgs struct {
	common.RewriterConfig
}

type store struct {
	args   StoreQueryArgs
	prefix string
}

// New creates a new filesystem Store from the provided URL.
func New(ep *url.URL) (stores.Store, error) {
	var s = &store{}
	s.prefix = ep.Path
	return s, parseStoreArgs(ep, &s.args)
}

func (s store) Provider() string {
	return "fs"
}

func (s store) SignGet(fragment pb.Fragment, _ time.Duration) (string, error) {
	return "file://" + s.args.RewritePath(s.prefix, fragment.ContentPath()), nil
}

func (s store) Exists(_ context.Context, fragment pb.Fragment) (bool, error) {
	var path = filepath.Join(FileSystemStoreRoot, filepath.FromSlash(s.args.RewritePath(s.prefix, fragment.ContentPath())))

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false, nil
	} else if err == nil {
		return true, nil
	} else {
		return false, err
	}
}

func (s store) Open(_ context.Context, fragment pb.Fragment) (io.ReadCloser, error) {
	var path = filepath.Join(FileSystemStoreRoot, filepath.FromSlash(s.args.RewritePath(s.prefix, fragment.ContentPath())))
	return os.Open(path)
}

func (s store) Persist(_ context.Context, spool stores.Spool) error {
	var path = filepath.Join(FileSystemStoreRoot, filepath.FromSlash(s.args.RewritePath(s.prefix, spool.GetFragment().ContentPath())))

	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return err
	}

	var f *os.File
	var err error
	f, err = os.CreateTemp(filepath.Dir(path), ".partial-"+filepath.Base(path))
	if err != nil {
		return err
	}

	defer func(name string) {
		if rmErr := os.Remove(f.Name()); rmErr != nil {
			log.WithFields(log.Fields{"err": rmErr, "path": path}).
				Warn("failed to cleanup temp file")
		}
	}(f.Name())

	switch spool.GetFragment().CompressionCodec {
	case pb.CompressionCodec_NONE:
		_, err = io.Copy(f, io.NewSectionReader(spool.File(), 0, spool.GetFragment().ContentLength()))
	default:
		_, err = io.Copy(f, io.NewSectionReader(spool.CompressedFile(), 0, spool.CompressedLength()))
	}

	if err == nil {
		err = f.Close()
	}
	if err == nil {
		err = os.Link(f.Name(), path)
	}
	return err
}

func (s store) List(_ context.Context, journal pb.Journal, callback func(pb.Fragment)) error {
	var dir = filepath.Join(FileSystemStoreRoot,
		filepath.FromSlash(s.args.RewritePath(s.prefix, journal.String()+"/")))

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil
	}
	return filepath.Walk(dir,
		func(name string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			} else if info.IsDir() {
				return nil // Descend into directory.
			} else if name, err = filepath.Rel(dir, name); err != nil {
				return err
			} else if name == "." || name == ".." {
				// Never return "." or ".." as they are not real directories.
				return nil
			}

			frag, err := pb.ParseFragmentFromRelativePath(journal, filepath.ToSlash(name))
			if err != nil {
				log.WithFields(log.Fields{
					"journal": journal,
					"name":    name,
					"err":     err,
				}).Warning("parsing fragment")
			} else if info.Size() == 0 && frag.ContentLength() > 0 {
				log.WithFields(log.Fields{
					"journal": journal,
					"name":    name,
				}).Warning("zero-length fragment")
			} else {
				frag.ModTime = info.ModTime().Unix()
				callback(frag)
			}
			return nil
		})
}

func (s store) Remove(_ context.Context, fragment pb.Fragment) error {
	var path = filepath.Join(FileSystemStoreRoot, filepath.FromSlash(s.args.RewritePath(s.prefix, fragment.ContentPath())))
	return os.Remove(path)
}

func (s store) IsAuthError(err error) bool {
	return os.IsPermission(err)
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