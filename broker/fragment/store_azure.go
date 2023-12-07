package fragment

import (
	"context"
	"errors"
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
	accountTenantID string // The tenant ID that owns the storage account that we're writing into
	// NOTE: This is not the tenant ID that owns the servie principal
	storageAccountName string // Storage accounts in Azure are the equivalent to a "bucket" in S3
	containerName      string // In azure, blobs are stored inside of containers, which live inside accounts
	prefix             string // This is the path prefix for the blobs inside the container

	RewriterConfig
}

func (cfg *AzureStoreConfig) serviceUrl() string {
	return fmt.Sprintf("https://%s.blob.core.windows.net", cfg.storageAccountName)
}

func (cfg *AzureStoreConfig) containerURL() string {
	return fmt.Sprintf("%s/%s", cfg.serviceUrl(), cfg.containerName)
}

type UdcAndExp struct {
	udc *service.UserDelegationCredential
	exp *time.Time
}

type azureBackend struct {
	// This is a cache of configured Pipelines for each tenant. These do not expire
	clients map[string]pipeline.Pipeline
	// This is a cache of Azure storage clients for each tenant. These do not expire
	svcClients map[string]service.Client
	clientMu   sync.Mutex
	// This is a cache of URL-signing credentials for each tenant. These DO expire
	udcs map[string]UdcAndExp
}

func (a *azureBackend) Provider() string {
	return "azure"
}

// See here for an example of how to use the Azure client libraries to create signatures:
// https://github.com/Azure/azure-sdk-for-go/blob/main/sdk/storage/azblob/service/examples_test.go#L285
func (a *azureBackend) SignGet(storageMapping *url.URL, fragment pb.Fragment, d time.Duration) (string, error) {
	cfg, err := parseAzureStorageMapping(storageMapping)
	if err != nil {
		return "", err
	}
	blobName := cfg.rewritePath(cfg.prefix, fragment.ContentPath())

	udc, err := a.getUserDelegationCredential(storageMapping)
	if err != nil {
		return "", err
	}

	sasQueryParams, err := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,       // Users MUST use HTTPS (not HTTP)
		ExpiryTime:    time.Now().UTC().Add(d), // Timestamps are expected in UTC https://docs.microsoft.com/en-us/rest/api/storageservices/create-service-sas#service-sas-example
		ContainerName: cfg.containerName,
		BlobName:      blobName,
		// These are the permissions granted to the signed URLs
		Permissions: to.Ptr(sas.BlobPermissions{Read: true}).String(),
	}.SignWithUserDelegation(udc)

	if err != nil {
		return "", err
	}

	log.WithFields(log.Fields{
		"tenantId":           cfg.accountTenantID,
		"storageAccountName": cfg.storageAccountName,
		"containerName":      cfg.containerName,
		"blobName":           blobName,
		"expiryTime":         sasQueryParams.ExpiryTime(),
	}).Debug("Signed get request")

	return fmt.Sprintf("%s/%s?%s", cfg.containerURL(), blobName, sasQueryParams.Encode()), nil
}

func (a *azureBackend) Exists(ctx context.Context, ep *url.URL, fragment pb.Fragment) (bool, error) {
	cfg, client, err := a.getAzurePipeline(ep)
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
	cfg, client, err := a.getAzurePipeline(ep)
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
	cfg, client, err := a.getAzurePipeline(ep)
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
	_, err = blobURL.Upload(ctx, body, headers, azblob.Metadata{}, azblob.BlobAccessConditions{}, azblob.DefaultAccessTier, azblob.BlobTagsMap{}, azblob.ClientProvidedKeyOptions{}, azblob.ImmutabilityPolicyOptions{})
	return err
}

func (a *azureBackend) List(ctx context.Context, store pb.FragmentStore, ep *url.URL, journal pb.Journal, callback func(pb.Fragment)) error {
	cfg, client, err := a.getAzurePipeline(ep)
	if err != nil {
		return err
	}
	u, err := url.Parse(cfg.containerURL())
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
				log.WithFields(log.Fields{
					"storageAccountName": cfg.storageAccountName,
					"name":               blob.Name,
					"err":                err,
				}).Warning("parsing fragment")
			} else if *(blob.Properties.ContentLength) == 0 && frag.ContentLength() > 0 {
				log.WithFields(log.Fields{
					"storageAccountName": cfg.storageAccountName,
					"name":               blob.Name,
				}).Warning("zero-length fragment")
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
	cfg, client, err := a.getAzurePipeline(fragment.BackingStore.URL())
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

// Turns an `azcore.TokenCredential` into an auto-refreshing `azblob.TokenCredential`
// This is useful for turning credentials coming from e.g `azidentity` into
// credentials that can be used with `azblob` Pipelines.
func getAzureStorageCredential(coreCredential azcore.TokenCredential, tenant string) (azblob.TokenCredential, error) {
	var tokenRefresher = func(credential azblob.TokenCredential) time.Duration {
		accessToken, err := coreCredential.GetToken(context.Background(), policy.TokenRequestOptions{TenantID: tenant, Scopes: []string{"https://storage.azure.com/.default"}})
		if err != nil {
			panic(err)
		}
		credential.SetToken(accessToken.Token)

		// Give 60s of padding in order to make sure we always have a non-expired token.
		// If we didn't do this, we would *begin* the refresh process as the token expires,
		// potentially leaving any consumer with an expired token while we fetch a new one.
		exp := accessToken.ExpiresOn.Sub(time.Now().Add(time.Minute))
		return exp
	}

	credential := azblob.NewTokenCredential("", tokenRefresher)
	return credential, nil
}

func parseAzureStorageMapping(storageMapping *url.URL) (cfg AzureStoreConfig, err error) {
	if err = parseStoreArgs(storageMapping, &cfg); err != nil {
		return
	}

	// Omit leading slash from URI. Note that FragmentStore already
	// enforces that URL Paths end in '/'.
	var splitPath = strings.Split(storageMapping.Path[1:], "/")

	if storageMapping.Scheme == "azure" {
		// Since only one non-ad "Shared Key" credential can be injected via
		// environment variables, we should only keep around one client for
		// all `azure://` requests.
		cfg.accountTenantID = "AZURE_SHARED_KEY"
		cfg.storageAccountName = os.Getenv("AZURE_ACCOUNT_NAME")
		cfg.containerName, cfg.prefix = storageMapping.Host, storageMapping.Path[1:]
	} else if storageMapping.Scheme == "azure-ad" {
		cfg.accountTenantID, cfg.storageAccountName, cfg.containerName, cfg.prefix = storageMapping.Host, splitPath[0], splitPath[1], strings.Join(splitPath[2:], "/")
	}

	return cfg, nil
}

func (a *azureBackend) getAzureServiceClient(storageMapping *url.URL) (client *service.Client, err error) {
	var cfg AzureStoreConfig

	if cfg, err = parseAzureStorageMapping(storageMapping); err != nil {
		return nil, err
	}

	if a.svcClients == nil {
		a.svcClients = make(map[string]service.Client)
	}

	if storageMapping.Scheme == "azure" {
		var accountName = os.Getenv("AZURE_ACCOUNT_NAME")
		var accountKey = os.Getenv("AZURE_ACCOUNT_KEY")

		if client, ok := a.svcClients[accountName]; ok {
			log.WithFields(log.Fields{
				"storageAccountName": accountName,
			}).Info("Re-using cached azure:// service client")
			return &client, nil
		}

		sharedKeyCred, err := service.NewSharedKeyCredential(accountName, accountKey)
		if err != nil {
			return nil, err
		}
		serviceClient, err := service.NewClientWithSharedKeyCredential(cfg.serviceUrl(), sharedKeyCred, &service.ClientOptions{})
		if err != nil {
			return nil, err
		}
		a.svcClients[accountName] = *serviceClient
		return serviceClient, nil
	} else if storageMapping.Scheme == "azure-ad" {
		// Link to the Azure docs describing what fields are required for active directory auth
		// https://learn.microsoft.com/en-us/azure/developer/go/azure-sdk-authentication-service-principal?tabs=azure-cli#-option-1-authenticate-with-a-secret
		var clientId = os.Getenv("AZURE_CLIENT_ID")
		var clientSecret = os.Getenv("AZURE_CLIENT_SECRET")

		if client, ok := a.svcClients[cfg.accountTenantID]; ok {
			log.WithFields(log.Fields{
				"accountTenantId": cfg.accountTenantID,
			}).Info("Re-using cached azure-ad:// service client")
			return &client, nil
		}

		identityCreds, err := azidentity.NewClientSecretCredential(
			cfg.accountTenantID,
			clientId,
			clientSecret,
			&azidentity.ClientSecretCredentialOptions{
				AdditionallyAllowedTenants: []string{cfg.accountTenantID},
				DisableInstanceDiscovery:   true,
			},
		)
		if err != nil {
			return nil, err
		}

		serviceClient, err := service.NewClient(cfg.serviceUrl(), identityCreds, &service.ClientOptions{})
		if err != nil {
			return nil, err
		}
		a.svcClients[cfg.accountTenantID] = *serviceClient
		return serviceClient, nil
	}
	return nil, errors.New("unrecognized URI scheme")
}

func (a *azureBackend) getAzurePipeline(ep *url.URL) (cfg AzureStoreConfig, client pipeline.Pipeline, err error) {
	if cfg, err = parseAzureStorageMapping(ep); err != nil {
		return
	}

	a.clientMu.Lock()
	defer a.clientMu.Unlock()

	if a.clients[cfg.accountTenantID] != nil {
		client = a.clients[cfg.accountTenantID]
		return
	}

	var credentials azblob.Credential

	if ep.Scheme == "azure" {
		var accountName = os.Getenv("AZURE_ACCOUNT_NAME")
		var accountKey = os.Getenv("AZURE_ACCOUNT_KEY")

		// Create an azblob credential that we can pass to `NewPipeline`
		credentials, err = azblob.NewSharedKeyCredential(accountName, accountKey)
		if err != nil {
			return cfg, nil, err
		}
	} else if ep.Scheme == "azure-ad" {
		// Link to the Azure docs describing what fields are required for active directory auth
		// https://learn.microsoft.com/en-us/azure/developer/go/azure-sdk-authentication-service-principal?tabs=azure-cli#-option-1-authenticate-with-a-secret
		var clientId = os.Getenv("AZURE_CLIENT_ID")
		var clientSecret = os.Getenv("AZURE_CLIENT_SECRET")

		identityCreds, err := azidentity.NewClientSecretCredential(
			cfg.accountTenantID,
			clientId,
			clientSecret,
			&azidentity.ClientSecretCredentialOptions{
				AdditionallyAllowedTenants: []string{cfg.accountTenantID},
				DisableInstanceDiscovery:   true,
			},
		)
		if err != nil {
			return cfg, nil, err
		}

		credentials, err = getAzureStorageCredential(identityCreds, cfg.accountTenantID)
		if err != nil {
			return cfg, nil, err
		}
	}

	client = azblob.NewPipeline(credentials, azblob.PipelineOptions{})
	if a.clients == nil {
		a.clients = make(map[string]pipeline.Pipeline)
	}
	a.clients[cfg.accountTenantID] = client

	log.WithFields(log.Fields{
		"tenant":               cfg.accountTenantID,
		"storageAccountName":   cfg.storageAccountName,
		"storageContainerName": cfg.containerName,
		"pathPrefix":           cfg.prefix,
	}).Info("constructed new Azure Storage pipeline client")

	return cfg, client, nil
}

func (a *azureBackend) buildBlobURL(cfg AzureStoreConfig, client pipeline.Pipeline, path string) (*azblob.BlockBlobURL, error) {
	u, err := url.Parse(fmt.Sprint(cfg.containerURL(), "/", cfg.rewritePath(cfg.prefix, path)))
	if err != nil {
		return nil, err
	}
	blobURL := azblob.NewBlockBlobURL(*u, client)
	return &blobURL, nil
}

// Cache UserDelegationCredentials and refresh them when needed
func (a *azureBackend) getUserDelegationCredential(storageMapping *url.URL) (*service.UserDelegationCredential, error) {
	if a.udcs == nil {
		a.udcs = make(map[string]UdcAndExp)
	}
	var cfg, err = parseAzureStorageMapping(storageMapping)
	if err != nil {
		return nil, err
	}
	var udc, hasCachedUdc = a.udcs[cfg.accountTenantID]

	// https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blob-user-delegation-sas-create-cli#use-azure-ad-credentials-to-secure-a-sas
	// According to the above docs, signed URLs generated with a UDC are invalid after
	// that UDC expires. In addition, a UDC can live up to 7 days. So let's ensure that
	// we always sign URLs with a UDC that has at least 5 days of useful life left in it.

	// ----| NOW |------|NOW+5Day|-----| udcExp |---- No need to refresh
	// ----| NOW  |-----| udcExp |-----|NOW+5Day|---- Need to refresh
	// ----|udcExp|-----|  NOW   | ------------------ Need to refresh
	if !hasCachedUdc || (udc.exp != nil && udc.exp.Before(time.Now().Add(time.Hour*24*5))) {
		// Generate UDCs that expire 6 days from now, and refresh them after they
		// have less than 5 days left until they expire.
		var startTime = time.Now().Add(time.Second * -10)
		var expTime = time.Now().Add(time.Hour * 24 * 6)
		var info = service.KeyInfo{
			Start:  to.Ptr(startTime.UTC().Format(sas.TimeFormat)),
			Expiry: to.Ptr(expTime.UTC().Format(sas.TimeFormat)),
		}

		var serviceClient, err = a.getAzureServiceClient(storageMapping)
		if err != nil {
			return nil, err
		}

		cred, err := serviceClient.GetUserDelegationCredential(context.Background(), info, nil)
		if err != nil {
			return nil, err
		}

		log.WithFields(log.Fields{
			"newExpiration":   expTime,
			"newStart":        startTime.String(),
			"service.KeyInfo": info,
			"tenant":          cfg.accountTenantID,
		}).Info("Refreshing Azure Storage UDC")

		udc = UdcAndExp{
			udc: cred,
			exp: &expTime,
		}
		a.udcs[cfg.accountTenantID] = udc
	}

	return udc.udc, nil
}
