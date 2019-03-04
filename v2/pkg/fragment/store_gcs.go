package fragment

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	log "github.com/sirupsen/logrus"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	gcsProvider = "gcs"
)

type gcsCfg struct {
	bucket string
	prefix string

	rewriterCfg
}

func gcsSignGET(ep *url.URL, fragment pb.Fragment, d time.Duration) (url string, err error) {
	defer instrument(gcsProvider, getSignedURLOp, err)
	cfg, _, opts, err := gcsClient(ep)
	if err != nil {
		return "", err
	}
	opts.Method = "GET"
	opts.Expires = time.Now().Add(d)

	url, err = storage.SignedURL(cfg.bucket, cfg.rewritePath(cfg.prefix, fragment.ContentPath()), &opts)
	return url, err
}

func gcsExists(ctx context.Context, ep *url.URL, fragment pb.Fragment) (exists bool, err error) {
	defer instrument(gcsProvider, existsOp, err)
	cfg, client, _, err := gcsClient(ep)
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

func gcsOpen(ctx context.Context, ep *url.URL, fragment pb.Fragment) (reader io.ReadCloser, err error) {
	defer instrument(gcsProvider, openOp, err)
	cfg, client, _, err := gcsClient(ep)
	if err != nil {
		return nil, err
	}
	reader, err = client.Bucket(cfg.bucket).Object(cfg.rewritePath(cfg.prefix, fragment.ContentPath())).NewReader(ctx)
	return reader, err
}

func gcsPersist(ctx context.Context, ep *url.URL, spool Spool) (err error) {
	defer instrument(gcsProvider, persistOp, err)
	cfg, client, _, err := gcsClient(ep)
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

func gcsList(ctx context.Context, store pb.FragmentStore, ep *url.URL, name pb.Journal, callback func(pb.Fragment)) (err error) {
	defer instrument(gcsProvider, listOp, err)
	cfg, client, _, err := gcsClient(ep)
	if err != nil {
		return err
	}
	var (
		it    = client.Bucket(cfg.bucket).Objects(ctx, &storage.Query{Prefix: cfg.rewritePath(cfg.prefix, name.String()) + "/"})
		strip = len(cfg.prefix)
		obj   *storage.ObjectAttrs
	)
	for obj, err = it.Next(); err == nil; obj, err = it.Next() {
		if frag, err2 := pb.ParseContentPath(obj.Name[strip:]); err2 != nil {
			log.WithFields(log.Fields{"bucket": cfg.bucket, "name": obj.Name, "err": err2}).Warning("parsing fragment")
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

func gcsRemove(ctx context.Context, fragment pb.Fragment) (err error) {
	defer instrument(gcsProvider, removeOp, err)
	cfg, client, _, err := gcsClient(fragment.BackingStore.URL())
	if err != nil {
		return err
	}
	err = client.Bucket(cfg.bucket).Object(cfg.rewritePath(cfg.prefix, fragment.ContentPath())).Delete(ctx)
	return err
}

func gcsClient(ep *url.URL) (cfg gcsCfg, client *storage.Client, opts storage.SignedURLOptions, err error) {
	if err = parseStoreArgs(ep, &cfg); err != nil {
		return
	}
	// Omit leading slash from bucket prefix. Note that FragmentStore already
	// enforces that URL Paths end in '/'.
	cfg.bucket, cfg.prefix = ep.Host, ep.Path[1:]

	sharedGCS.Lock()
	defer sharedGCS.Unlock()

	if sharedGCS.Client != nil {
		client = sharedGCS.Client
		opts = sharedGCS.SignedURLOptions
		return
	}
	var ctx = context.Background()

	creds, err := google.FindDefaultCredentials(ctx, storage.ScopeFullControl)
	if err != nil {
		return
	} else if creds.JSON == nil {
		err = fmt.Errorf("use of GCS requires that a service-account private key be supplied with application default credentials")
		return
	}
	conf, err := google.JWTConfigFromJSON(creds.JSON, storage.ScopeFullControl)
	if err != nil {
		return
	}
	client, err = storage.NewClient(ctx, option.WithTokenSource(conf.TokenSource(ctx)))
	if err != nil {
		return
	}
	opts = storage.SignedURLOptions{
		GoogleAccessID: conf.Email,
		PrivateKey:     conf.PrivateKey,
	}
	sharedGCS.Client, sharedGCS.SignedURLOptions = client, opts

	log.WithFields(log.Fields{
		"ProjectID":      creds.ProjectID,
		"GoogleAccessID": conf.Email,
		"PrivateKeyID":   conf.PrivateKeyID,
		"Subject":        conf.Subject,
		"Scopes":         conf.Scopes,
	}).Info("constructed new GCS client")

	return
}

var sharedGCS struct {
	*storage.Client
	storage.SignedURLOptions
	sync.Mutex
}
