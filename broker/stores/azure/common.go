package azure

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/gorilla/schema"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/stores"
	"go.gazette.dev/core/broker/stores/common"
)

// StoreQueryArgs contains fields that are parsed from the query arguments
// of an azure:// or azure-ad:// fragment store URL.
type StoreQueryArgs struct {
	common.RewriterConfig
}

// storeBase provides common Azure storage operations
type storeBase struct {
	args           StoreQueryArgs
	storageAccount string // Storage accounts in Azure are the equivalent to a "bucket" in S3
	container      string // In azure, blobs are stored inside of containers, which live inside accounts
	prefix         string // This is the path prefix for the blobs inside the container
	pipeline       pipeline.Pipeline
}

func (a *storeBase) Provider() string {
	return "azure"
}

func (a *storeBase) Exists(ctx context.Context, fragment pb.Fragment) (bool, error) {
	var blobURL, err = a.buildBlobURL(fragment.ContentPath())
	if err != nil {
		return false, err
	}
	if _, err = blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{}); err == nil {
		return true, nil
	}
	if inner, ok := err.(azblob.StorageError); ok && inner.ServiceCode() == azblob.ServiceCodeBlobNotFound {
		return false, nil
	}
	return false, err
}

func (a *storeBase) Open(ctx context.Context, fragment pb.Fragment) (io.ReadCloser, error) {
	var blobURL, err = a.buildBlobURL(fragment.ContentPath())
	if err != nil {
		return nil, err
	}
	download, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, err
	}
	return download.Body(azblob.RetryReaderOptions{}), nil
}

func (a *storeBase) Persist(ctx context.Context, spool stores.Spool) error {
	var blobURL, err = a.buildBlobURL(spool.GetFragment().ContentPath())
	if err != nil {
		return err
	}
	var headers = azblob.BlobHTTPHeaders{}
	var body io.ReadSeeker
	if spool.GetFragment().CompressionCodec == pb.CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION {
		headers.ContentEncoding = "gzip"
	}
	if spool.GetFragment().CompressionCodec != pb.CompressionCodec_NONE {
		body = io.NewSectionReader(spool.CompressedFile(), 0, spool.CompressedLength())
	} else {
		body = io.NewSectionReader(spool.File(), 0, spool.GetFragment().ContentLength())
	}
	_, err = blobURL.Upload(ctx, body, headers, azblob.Metadata{}, azblob.BlobAccessConditions{}, azblob.DefaultAccessTier, azblob.BlobTagsMap{}, azblob.ClientProvidedKeyOptions{}, azblob.ImmutabilityPolicyOptions{})
	return err
}

func (a *storeBase) List(ctx context.Context, journal pb.Journal, callback func(pb.Fragment)) error {
	var u, err = url.Parse(a.containerURL())
	if err != nil {
		return err
	}
	var containerURL = azblob.NewContainerURL(*u, a.pipeline)
	var options = azblob.ListBlobsSegmentOptions{Prefix: a.args.RewritePath(a.prefix, journal.String()) + "/"}
	for marker := (azblob.Marker{}); marker.NotDone(); {
		var segmentList, err = containerURL.ListBlobsFlatSegment(ctx, marker, options)
		if err != nil {
			return err
		}
		for _, blob := range segmentList.Segment.BlobItems {
			if strings.HasSuffix(blob.Name, "/") {
			} else if frag, err := pb.ParseFragmentFromRelativePath(journal, blob.Name[len(*segmentList.Prefix):]); err != nil {
				log.WithFields(log.Fields{
					"storageAccountName": a.storageAccount,
					"name":               blob.Name,
					"err":                err,
				}).Warning("parsing fragment")
			} else if *(blob.Properties.ContentLength) == 0 && frag.ContentLength() > 0 {
				log.WithFields(log.Fields{
					"storageAccountName": a.storageAccount,
					"name":               blob.Name,
				}).Warning("zero-length fragment")
			} else {
				frag.ModTime = blob.Properties.LastModified.Unix()
				callback(frag)
			}
		}
		marker = segmentList.NextMarker
	}
	return nil
}

func (a *storeBase) Remove(ctx context.Context, fragment pb.Fragment) error {
	var blobURL, err = a.buildBlobURL(fragment.ContentPath())
	if err != nil {
		return err
	}
	_, err = blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	return err
}

func (a *storeBase) IsAuthError(err error) bool {
	if err == nil {
		return false
	}

	if storageErr, ok := err.(azblob.StorageError); ok {
		switch storageErr.ServiceCode() {
		case azblob.ServiceCodeContainerNotFound,
			azblob.ServiceCodeContainerDisabled,
			azblob.ServiceCodeAccountIsDisabled:
			return true
		}

		if storageErr.Response() != nil {
			switch storageErr.Response().StatusCode {
			case http.StatusForbidden:
				return true
			}
		}
	}

	return false
}

func (a *storeBase) buildBlobURL(path string) (*azblob.BlockBlobURL, error) {
	var u, err = url.Parse(fmt.Sprint(a.containerURL(), "/", a.args.RewritePath(a.prefix, path)))
	if err != nil {
		return nil, err
	}
	var blobURL = azblob.NewBlockBlobURL(*u, a.pipeline)
	return &blobURL, nil
}

func azureStorageURL(storageAccount string) string {
	return fmt.Sprintf("https://%s.blob.core.windows.net", storageAccount)
}

func (a *storeBase) containerURL() string {
	return fmt.Sprintf("%s/%s", azureStorageURL(a.storageAccount), a.container)
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