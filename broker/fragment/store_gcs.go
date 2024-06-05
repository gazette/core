package fragment

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GSStoreConfig configures a Fragment store of the "gs://" scheme.
// It is initialized from parsed URL parameters of the pb.FragmentStore.
type GSStoreConfig struct {
	bucket string
	prefix string

	RewriterConfig
}

type gcsBackend struct {
	client           *storage.Client
	signedURLOptions storage.SignedURLOptions
	clientMu         sync.Mutex
}

func (s *gcsBackend) Provider() string {
	return "gcs"
}

func (s *gcsBackend) SignGet(ep *url.URL, fragment pb.Fragment, d time.Duration) (string, error) {
	cfg, client, opts, err := s.gcsClient(ep)
	if err != nil {
		return "", err
	}

	if DisableSignedUrls {
		u := &url.URL{
			Path: fmt.Sprintf("/%s/%s", cfg.bucket, cfg.rewritePath(cfg.prefix, fragment.ContentPath())),
		}
		u.Scheme = "https"
		u.Host = "storage.googleapis.com"

		return u.String(), nil
	} else {
		opts.Method = "GET"
		opts.Expires = time.Now().Add(d)

		return client.Bucket(cfg.bucket).SignedURL(cfg.rewritePath(cfg.prefix, fragment.ContentPath()), &opts)
	}
}

func (s *gcsBackend) Exists(ctx context.Context, ep *url.URL, fragment pb.Fragment) (exists bool, err error) {
	cfg, client, _, err := s.gcsClient(ep)
	if err != nil {
		return false, err
	}
	_, err = client.Bucket(cfg.bucket).Object(cfg.rewritePath(cfg.prefix, fragment.ContentPath())).Attrs(ctx)
	if err == nil {
		exists = true
	} else if err == storage.ErrObjectNotExist {
		err = nil
	}
	return exists, err
}

func (s *gcsBackend) Open(ctx context.Context, ep *url.URL, fragment pb.Fragment) (io.ReadCloser, error) {
	cfg, client, _, err := s.gcsClient(ep)
	if err != nil {
		return nil, err
	}
	return client.Bucket(cfg.bucket).Object(cfg.rewritePath(cfg.prefix, fragment.ContentPath())).NewReader(ctx)
}

func (s *gcsBackend) Persist(ctx context.Context, ep *url.URL, spool Spool) error {
	cfg, client, _, err := s.gcsClient(ep)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	var wc = client.Bucket(cfg.bucket).Object(cfg.rewritePath(cfg.prefix, spool.ContentPath())).NewWriter(ctx)

	if spool.CompressionCodec == pb.CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION {
		wc.ContentEncoding = "gzip"
	}
	if spool.CompressionCodec != pb.CompressionCodec_NONE {
		_, err = io.Copy(wc, io.NewSectionReader(spool.compressedFile, 0, spool.compressedLength))
	} else {
		_, err = io.Copy(wc, io.NewSectionReader(spool.File, 0, spool.ContentLength()))
	}
	if err != nil {
		cancel() // Abort |wc|.
	} else {
		err = wc.Close()
	}
	return err
}

func (s *gcsBackend) List(ctx context.Context, store pb.FragmentStore, ep *url.URL, journal pb.Journal, callback func(pb.Fragment)) error {
	var cfg, client, _, err = s.gcsClient(ep)
	if err != nil {
		return err
	}
	var (
		q = storage.Query{
			Prefix: cfg.rewritePath(cfg.prefix, journal.String()) + "/",
		}
		it  = client.Bucket(cfg.bucket).Objects(ctx, &q)
		obj *storage.ObjectAttrs
	)
	for obj, err = it.Next(); err == nil; obj, err = it.Next() {
		if strings.HasSuffix(obj.Name, "/") {
			// Ignore directory-like objects, usually created by mounting buckets with a FUSE driver.
		} else if frag, err := pb.ParseFragmentFromRelativePath(journal, obj.Name[len(q.Prefix):]); err != nil {
			log.WithFields(log.Fields{"bucket": cfg.bucket, "name": obj.Name, "err": err}).Warning("parsing fragment")
		} else if obj.Size == 0 && frag.ContentLength() > 0 {
			log.WithFields(log.Fields{"bucket": cfg.bucket, "name": obj.Name}).Warning("zero-length fragment")
		} else {
			frag.ModTime = obj.Updated.Unix()
			frag.BackingStore = store
			callback(frag)
		}
	}
	if err == iterator.Done {
		err = nil
	}
	return err
}

func (s *gcsBackend) Remove(ctx context.Context, fragment pb.Fragment) error {
	cfg, client, _, err := s.gcsClient(fragment.BackingStore.URL())
	if err != nil {
		return err
	}
	return client.Bucket(cfg.bucket).Object(cfg.rewritePath(cfg.prefix, fragment.ContentPath())).Delete(ctx)
}
