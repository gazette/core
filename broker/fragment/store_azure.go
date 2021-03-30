package fragment

import (
	"context"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// AzureStoreConfig configures a Fragment store of the "azure://" scheme.
// It is initialized from parsed URL parametrs of the pb.FragmentStore
type AzureStoreConfig struct {
	bucket string // in Azure buckets are called "containers"
	prefix string

	RewriterConfig
}

type azureBackend struct{
	endpoint string
	client pipeline.Pipeline
	clientMu sync.Mutex
}

func (a *azureBackend) Provider() string {
	return "azure"
}

func (a *azureBackend) SignGet(ep *url.URL, fragment pb.Fragment, d time.Duration) (string, error) {
	panic("implement me")
}

func (a *azureBackend) Exists(ctx context.Context, ep *url.URL, fragment pb.Fragment) (bool, error) {
	cfg, client, err := a.azureClient(ep)
	if err != nil {
		return false, err
	}
	u, err := url.Parse(fmt.Sprint(a.endpoint, cfg.bucket, cfg.rewritePath(cfg.prefix, fragment.ContentPath())))
	if err != nil {
		return false, err
	}
	blobURL := azblob.NewBlockBlobURL(*u, client)
	if _, err = blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{}); err == nil {
		return true, nil
	} else {
		storageErr := err.(azblob.StorageError)
		errCode := storageErr.ServiceCode()
		if errCode == azblob.ServiceCodeBlobNotFound {
			return false, nil
		}
		return false, storageErr
	}
}

func (a *azureBackend) Open(ctx context.Context, ep *url.URL, fragment pb.Fragment) (io.ReadCloser, error) {
	cfg, client, err := a.azureClient(ep)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(fmt.Sprint(a.endpoint, cfg.bucket, cfg.rewritePath(cfg.prefix, fragment.ContentPath())))
	if err != nil {
		return nil, err
	}
	blobURL := azblob.NewBlockBlobURL(*u, client)
	download, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, err
	}
	return download.Body(azblob.RetryReaderOptions{MaxRetryRequests: 20}), nil
}

func (a *azureBackend) Persist(ctx context.Context, ep *url.URL, spool Spool) error {
	cfg, client, err := a.azureClient(ep)
	if err != nil {
		return err
	}
	u, err := url.Parse(fmt.Sprint(a.endpoint, cfg.bucket, cfg.rewritePath(cfg.prefix, spool.ContentPath())))
	if err != nil {
		return err
	}
	blobURL := azblob.NewBlockBlobURL(*u, client)
	headers := azblob.BlobHTTPHeaders{}
	var body io.ReadSeeker
	if spool.CompressionCodec == pb.CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION {
		headers.ContentEncoding = "gzip"
	}
	if spool.CompressionCodec != pb.CompressionCodec_NONE {
		body = io.NewSectionReader(spool.compressedFile, 0, spool.compressedLength)
	} else {
		body = io.NewSectionReader(spool.File, 0, spool.ContentLength())
	}
	_, err = blobURL.Upload(ctx, body, headers, azblob.Metadata{}, azblob.BlobAccessConditions{}, azblob.DefaultAccessTier, azblob.BlobTagsMap{}, azblob.ClientProvidedKeyOptions{})
	return err
}

func (a *azureBackend) List(ctx context.Context, store pb.FragmentStore, ep *url.URL, journal pb.Journal, callback func(pb.Fragment)) error {
	cfg, client, err := a.azureClient(ep)
	if err != nil {
		return err
	}
	u, err := url.Parse(fmt.Sprint(a.endpoint, cfg.bucket))
	if err != nil {
		return err
	}
	containerURL := azblob.NewContainerURL(*u, client)
	options := azblob.ListBlobsSegmentOptions{Prefix: cfg.rewritePath(cfg.prefix, journal.String()) + "/"}
	for marker := (azblob.Marker{}); marker.NotDone(); {
		segmentList, err := containerURL.ListBlobsFlatSegment(ctx, marker, options)
		if err != nil {
			return err
		}
		for _, blob := range segmentList.Segment.BlobItems {
			if strings.HasSuffix(blob.Name, "/") {
				//Ignore directory-like objects, usually created by mounting buckets with a FUSE driver.
			} else if frag, err := pb.ParseFragmentFromRelativePath(journal, blob.Name[len(*segmentList.Prefix):]); err != nil {
				log.WithFields(log.Fields{"bucket": cfg.bucket, "name": blob.Name, "err": err}).Warning("parsing fragment")
			} else if *(blob.Properties.ContentLength) == 0 && frag.ContentLength() > 0 {
				log.WithFields(log.Fields{"bucket": cfg.bucket, "name": blob.Name}).Warning("zero-length fragment")
			} else {
				frag.ModTime = blob.Properties.LastModified.Unix()
				frag.BackingStore = store
				callback(frag)
			}
		}
		marker = segmentList.NextMarker
	}
	return nil
}

func (a *azureBackend) Remove(ctx context.Context, fragment pb.Fragment) error {
	cfg, client, err := a.azureClient(fragment.BackingStore.URL())
	if err != nil {
		return err
	}
	u, err := url.Parse(fmt.Sprint(a.endpoint, cfg.bucket, cfg.rewritePath(cfg.prefix, fragment.ContentPath())))
	if err != nil {
		return err
	}
	blobURL := azblob.NewBlockBlobURL(*u, client)
	_, err = blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	return err
}


func (a *azureBackend) azureClient(ep *url.URL) (cfg AzureStoreConfig, client pipeline.Pipeline, err error) {
	if err = parseStoreArgs(ep, &cfg); err != nil {
		return
	}
	// Omit leading slash from bucket prefix. Note that FragmentStore already
	// enforces that URL Paths end in '/'.
	cfg.bucket, cfg.prefix = ep.Host, ep.Path[1:]

	a.clientMu.Lock()
	defer a.clientMu.Unlock()

	if a.client != nil {
		client = a.client
		return
	}

	accountName := os.Getenv("AZURE_ACCOUNT_NAME")
	accountKey := os.Getenv("AZURE_ACCOUNT_KEY")
	credentials, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return
	}
	client = azblob.NewPipeline(credentials, azblob.PipelineOptions{})
	a.client = client
	a.endpoint = fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)


	log.WithFields(log.Fields{
		"Account Name":      accountName,
	}).Info("constructed new Azure Storage client")

	return
}