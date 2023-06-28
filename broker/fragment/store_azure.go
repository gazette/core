package fragment

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-blob-go/azblob"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
)

// AzureStoreConfig configures a Fragment store of the "azure://" or "azure-ad://" scheme.
// It is initialized from parsed URL parametrs of the pb.FragmentStore
type AzureStoreConfig struct {
	bucket string // in Azure buckets are called "containers"
	prefix string

	RewriterConfig
}

type azureBackend struct {
	endpoint    string
	accountName string
	client      pipeline.Pipeline
	svcClient   service.Client
	clientMu    sync.Mutex
}

func (a *azureBackend) Provider() string {
	return "azure"
}

// https://github.com/Azure/azure-sdk-for-go/blob/main/sdk/storage/azblob/service/examples_test.go#L285
func (a *azureBackend) SignGet(ep *url.URL, fragment pb.Fragment, d time.Duration) (string, error) {
	cfg, _, err := a.azureClient(ep)
	if err != nil {
		return "", err
	}
	blobName := cfg.rewritePath(cfg.prefix, fragment.ContentPath())

	// Set current and past time and create key
	currentTime := time.Now().UTC().Add(-10 * time.Second)
	expTime := currentTime.Add(d)
	info := service.KeyInfo{
		Start:  to.Ptr(currentTime.UTC().Format(sas.TimeFormat)),
		Expiry: to.Ptr(expTime.UTC().Format(sas.TimeFormat)),
	}

	udc, err := a.svcClient.GetUserDelegationCredential(context.Background(), info, nil)

	if err != nil {
		return "", err
	}

	sasQueryParams, err := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS, // Users MUST use HTTPS (not HTTP)
		StartTime:     currentTime,
		ExpiryTime:    expTime, // Timestamps are expected in UTC https://docs.microsoft.com/en-us/rest/api/storageservices/create-service-sas#service-sas-example
		ContainerName: cfg.bucket,
		BlobName:      blobName,

		// To produce a container SAS (as opposed to a blob SAS), assign to Permissions using
		// ContainerSASPermissions and make sure the BlobName field is "" (the default).
		Permissions: to.Ptr(sas.ContainerPermissions{Read: true, Add: true, Write: true}).String(),
	}.SignWithUserDelegation(udc)

	if err != nil {
		return "", err
	}
	return fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s?%s", a.accountName, cfg.bucket, blobName, sasQueryParams.Encode()), nil
}

func (a *azureBackend) Exists(ctx context.Context, ep *url.URL, fragment pb.Fragment) (bool, error) {
	cfg, client, err := a.azureClient(ep)
	if err != nil {
		return false, err
	}
	blobURL, err := a.buildBlobURL(cfg, client, fragment.ContentPath())
	if err != nil {
		return false, err
	}
	if _, err = blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{}); err == nil {
		return true, nil
	}
	storageErr, ok := err.(azblob.StorageError)
	if !ok {
		return false, err
	}
	if storageErr.ServiceCode() == azblob.ServiceCodeBlobNotFound {
		return false, nil
	}
	return false, storageErr
}

func (a *azureBackend) Open(ctx context.Context, ep *url.URL, fragment pb.Fragment) (io.ReadCloser, error) {
	cfg, client, err := a.azureClient(ep)
	if err != nil {
		return nil, err
	}
	blobURL, err := a.buildBlobURL(cfg, client, fragment.ContentPath())
	if err != nil {
		return nil, err
	}
	download, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, err
	}
	return download.Body(azblob.RetryReaderOptions{}), nil
}

func (a *azureBackend) Persist(ctx context.Context, ep *url.URL, spool Spool) error {
	cfg, client, err := a.azureClient(ep)
	if err != nil {
		return err
	}
	blobURL, err := a.buildBlobURL(cfg, client, spool.ContentPath())
	if err != nil {
		return err
	}
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
	u, err := url.Parse(fmt.Sprint(a.endpoint, cfg.bucket, "/"))
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
	blobURL, err := a.buildBlobURL(cfg, client, fragment.ContentPath())
	if err != nil {
		return err
	}
	_, err = blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	return err
}

// https://gist.github.com/ItalyPaleAle/ec6498bfa81a96f9ca27a2da6f60a770
func GetAzureStorageCredential(coreCredential azcore.TokenCredential) (azblob.TokenCredential, error) {
	var tokenRefresher azblob.TokenRefresher
	tokenRefresher = func(credential azblob.TokenCredential) time.Duration {
		accessToken, err := coreCredential.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{"https://storage.azure.com/.default"}})
		if err != nil {
			panic(err)
		}
		credential.SetToken(accessToken.Token)

		exp := accessToken.ExpiresOn.Sub(time.Now().Add(2 * time.Minute))
		return exp
	}

	credential := azblob.NewTokenCredential("", tokenRefresher)
	return credential, nil
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

	var credentials azblob.Credential

	accountName := os.Getenv("AZURE_ACCOUNT_NAME")
	a.endpoint = fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)

	if ep.Scheme == "azure" {
		accountKey := os.Getenv("AZURE_ACCOUNT_KEY")
		sharedKeyCred, cred_err := service.NewSharedKeyCredential(accountName, accountKey)
		if cred_err != nil {
			return cfg, nil, cred_err
		}
		serviceClient, cred_err := service.NewClientWithSharedKeyCredential(a.endpoint, sharedKeyCred, &service.ClientOptions{})
		if cred_err != nil {
			return cfg, nil, cred_err
		}
		a.svcClient = *serviceClient
		// azblob SharedKeyCredential is apparently different from service SharedKeyCredential
		credentials, cred_err = azblob.NewSharedKeyCredential(accountName, accountKey)
		if cred_err != nil {
			return cfg, nil, cred_err
		}
	} else if ep.Scheme == "azure-ad" {
		// https://learn.microsoft.com/en-us/azure/developer/go/azure-sdk-authentication-service-principal?tabs=azure-cli#-option-1-authenticate-with-a-secret
		tenantId := os.Getenv("AZURE_TENANT_ID")
		clientId := os.Getenv("AZURE_CLIENT_ID")
		clientSecret := os.Getenv("AZURE_CLIENT_SECRET")

		identityCreds, cred_err := azidentity.NewClientSecretCredential(tenantId, clientId, clientSecret, &azidentity.ClientSecretCredentialOptions{AdditionallyAllowedTenants: []string{"*"}})
		if cred_err != nil {
			return cfg, nil, cred_err
		}

		serviceClient, cred_err := service.NewClient(a.endpoint, identityCreds, &service.ClientOptions{})
		if cred_err != nil {
			return cfg, nil, cred_err
		}
		a.svcClient = *serviceClient
	}

	client = azblob.NewPipeline(credentials, azblob.PipelineOptions{})
	a.client = client
	a.accountName = accountName

	log.WithFields(log.Fields{
		"Account Name": accountName,
	}).Info("constructed new Azure Storage client")

	return
}

func (a *azureBackend) buildBlobURL(cfg AzureStoreConfig, client pipeline.Pipeline, path string) (*azblob.BlockBlobURL, error) {
	u, err := url.Parse(fmt.Sprint(a.endpoint, cfg.bucket, "/", cfg.rewritePath(cfg.prefix, path)))
	if err != nil {
		return nil, err
	}
	blobURL := azblob.NewBlockBlobURL(*u, client)
	return &blobURL, nil
}
