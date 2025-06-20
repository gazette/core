package fragment

import (
	"context"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"

	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
)

// FileSystemStoreRoot is the filesystem path which roots fragment ContentPaths
// of a file:// fragment store. It must be set at program startup prior to use.
var FileSystemStoreRoot = "/dev/null/must/configure/file/store/root"

// FileStoreQueryArgs contains fields that are parsed from the query arguments
// of a file:// fragment store URL.
type FileStoreQueryArgs struct {
	RewriterConfig
}

type fsStore struct {
	args   FileStoreQueryArgs
	prefix string
}

func newFSStore(ep *url.URL) (*fsStore, error) {
	var s = &fsStore{}
	s.prefix = ep.Path
	return s, parseStoreArgs(ep, &s.args)
}

func (s fsStore) Provider() string {
	return "fs"
}

func (s fsStore) SignGet(fragment pb.Fragment, _ time.Duration) (string, error) {
	return "file://" + s.args.rewritePath(s.prefix, fragment.ContentPath()), nil
}

func (s fsStore) Exists(_ context.Context, fragment pb.Fragment) (bool, error) {
	var path = filepath.Join(FileSystemStoreRoot, filepath.FromSlash(s.args.rewritePath(s.prefix, fragment.ContentPath())))

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false, nil
	} else if err == nil {
		return true, nil
	} else {
		return false, err
	}
}

func (s fsStore) Open(_ context.Context, fragment pb.Fragment) (io.ReadCloser, error) {
	var path = filepath.Join(FileSystemStoreRoot, filepath.FromSlash(s.args.rewritePath(s.prefix, fragment.ContentPath())))
	return os.Open(path)
}

func (s fsStore) Persist(_ context.Context, spool Spool) error {
	var path = filepath.Join(FileSystemStoreRoot, filepath.FromSlash(s.args.rewritePath(s.prefix, spool.ContentPath())))

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

	switch spool.CompressionCodec {
	case pb.CompressionCodec_NONE:
		_, err = io.Copy(f, io.NewSectionReader(spool.File, 0, spool.ContentLength()))
	default:
		_, err = io.Copy(f, io.NewSectionReader(spool.compressedFile, 0, spool.compressedLength))
	}

	if err == nil {
		err = f.Close()
	}
	if err == nil {
		err = os.Link(f.Name(), path)
	}
	return err
}

func (s fsStore) List(_ context.Context, journal pb.Journal, callback func(pb.Fragment)) error {
	var dir = filepath.Join(FileSystemStoreRoot,
		filepath.FromSlash(s.args.rewritePath(s.prefix, journal.String()+"/")))

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

func (s fsStore) Remove(_ context.Context, fragment pb.Fragment) error {
	var path = filepath.Join(FileSystemStoreRoot, filepath.FromSlash(s.args.rewritePath(s.prefix, fragment.ContentPath())))
	return os.Remove(path)
}

func (s fsStore) IsAuthError(err error) bool {
	return os.IsPermission(err)
}
