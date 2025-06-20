package fragment

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCSStoreQueryArgs contains fields that are parsed from the query arguments
// of a gs:// fragment store URL.
type GCSStoreQueryArgs struct {
	RewriterConfig
}

type gcsStore struct {
	bucket           string
	prefix           string
	args             GCSStoreQueryArgs
	client           *storage.Client
	signedURLOptions storage.SignedURLOptions
}

func newGCSStore(ep *url.URL) (*gcsStore, error) {
	var args GCSStoreQueryArgs
	if err := parseStoreArgs(ep, &args); err != nil {
		return nil, err
	}
	// Omit leading slash from bucket prefix. Note that FragmentStore already
	// enforces that URL Paths end in '/'.
	var bucket, prefix = ep.Host, ep.Path[1:]

	var ctx = context.Background()

	creds, err := google.FindDefaultCredentials(ctx, storage.ScopeFullControl)
	if err != nil {
		return nil, err
	} else if creds.JSON == nil {
		return nil, fmt.Errorf("use of GCS requires that a service-account private key be supplied with application default credentials")
	}
	conf, err := google.JWTConfigFromJSON(creds.JSON, storage.ScopeFullControl)
	if err != nil {
		return nil, err
	}
	client, err := storage.NewClient(ctx,
		option.WithTokenSource(conf.TokenSource(ctx)),
		option.WithHTTPClient(httpClientDisableCompression))
	if err != nil {
		return nil, err
	}
	var opts = storage.SignedURLOptions{
		GoogleAccessID: conf.Email,
		PrivateKey:     conf.PrivateKey,
	}

	log.WithFields(log.Fields{
		"ProjectID":      creds.ProjectID,
		"GoogleAccessID": conf.Email,
		"PrivateKeyID":   conf.PrivateKeyID,
		"Subject":        conf.Subject,
		"Scopes":         conf.Scopes,
	}).Info("constructed new GCS client")

	return &gcsStore{
		bucket:           bucket,
		prefix:           prefix,
		args:             args,
		client:           client,
		signedURLOptions: opts,
	}, nil
}

func (s *gcsStore) Provider() string {
	return "gcs"
}

func (s *gcsStore) SignGet(fragment pb.Fragment, d time.Duration) (string, error) {
	var opts = s.signedURLOptions
	opts.Method = "GET"
	opts.Expires = time.Now().Add(d)

	return storage.SignedURL(s.bucket, s.args.rewritePath(s.prefix, fragment.ContentPath()), &opts)
}

func (s *gcsStore) Exists(ctx context.Context, fragment pb.Fragment) (exists bool, err error) {
	_, err = s.client.Bucket(s.bucket).Object(s.args.rewritePath(s.prefix, fragment.ContentPath())).Attrs(ctx)
	if err == nil {
		exists = true
	} else if err == storage.ErrObjectNotExist {
		err = nil
	}
	return exists, err
}

func (s *gcsStore) Open(ctx context.Context, fragment pb.Fragment) (io.ReadCloser, error) {
	return s.client.Bucket(s.bucket).Object(s.args.rewritePath(s.prefix, fragment.ContentPath())).NewReader(ctx)
}

func (s *gcsStore) Persist(ctx context.Context, spool Spool) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wc = s.client.Bucket(s.bucket).Object(s.args.rewritePath(s.prefix, spool.ContentPath())).NewWriter(ctx)

	if spool.CompressionCodec == pb.CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION {
		wc.ContentEncoding = "gzip"
	}
	var err error
	if spool.CompressionCodec != pb.CompressionCodec_NONE {
		_, err = io.Copy(wc, io.NewSectionReader(spool.compressedFile, 0, spool.compressedLength))
	} else {
		_, err = io.Copy(wc, io.NewSectionReader(spool.File, 0, spool.ContentLength()))
	}
	if err != nil {
		return err
	}
	return wc.Close()
}

func (s *gcsStore) List(ctx context.Context, journal pb.Journal, callback func(pb.Fragment)) error {
	var (
		q = storage.Query{
			Prefix: s.args.rewritePath(s.prefix, journal.String()) + "/",
		}
		it  = s.client.Bucket(s.bucket).Objects(ctx, &q)
		obj *storage.ObjectAttrs
		err error
	)
	for obj, err = it.Next(); err == nil; obj, err = it.Next() {
		if strings.HasSuffix(obj.Name, "/") {
			// Ignore directory-like objects, usually created by mounting buckets with a FUSE driver.
		} else if frag, err := pb.ParseFragmentFromRelativePath(journal, obj.Name[len(q.Prefix):]); err != nil {
			log.WithFields(log.Fields{"bucket": s.bucket, "name": obj.Name, "err": err}).Warning("parsing fragment")
		} else if obj.Size == 0 && frag.ContentLength() > 0 {
			log.WithFields(log.Fields{"bucket": s.bucket, "name": obj.Name}).Warning("zero-length fragment")
		} else {
			frag.ModTime = obj.Updated.Unix()
			callback(frag)
		}
	}
	if err == iterator.Done {
		err = nil
	}
	return err
}

func (s *gcsStore) Remove(ctx context.Context, fragment pb.Fragment) error {
	return s.client.Bucket(s.bucket).Object(s.args.rewritePath(s.prefix, fragment.ContentPath())).Delete(ctx)
}

func (s *gcsStore) IsAuthError(err error) bool {
	if err == nil {
		return false
	}

	if err == storage.ErrBucketNotExist {
		return true
	}

	// Check for Google API errors that indicate AuthZ failures.
	var gErr *googleapi.Error
	if errors.As(err, &gErr) {
		switch gErr.Code {
		case http.StatusForbidden:
			return true
		case http.StatusNotFound:
			// Only treat bucket-level 404s as AuthZ failures, not object-level.
			if strings.Contains(gErr.Message, "bucket") {
				return true
			}
		}
	}

	return false
}
