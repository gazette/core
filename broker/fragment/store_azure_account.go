package fragment

import (
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-blob-go/azblob"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
)

// azureAccountStore implements the Store interface for Azure Blob Storage
// using Shared Key authentication (azure:// scheme)
type azureAccountStore struct {
	azureStoreBase
	sasKey *service.SharedKeyCredential
}

func newAzureAccountStore(ep *url.URL) (*azureAccountStore, error) {
	var args AzureQueryArgs

	if err := parseStoreArgs(ep, &args); err != nil {
		return nil, err
	}

	var container = ep.Host
	var prefix = ep.Path[1:]

	var storageAccount = os.Getenv("AZURE_ACCOUNT_NAME")
	var accountKey = os.Getenv("AZURE_ACCOUNT_KEY")

	if storageAccount == "" || accountKey == "" {
		return nil, fmt.Errorf("AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY must be set for azure:// URLs")
	}

	credentials, err := azblob.NewSharedKeyCredential(storageAccount, accountKey)
	if err != nil {
		return nil, err
	}

	var pipeline = azblob.NewPipeline(credentials, azblob.PipelineOptions{})

	// Create the new SDK credential for SAS signing
	sasKey, err := service.NewSharedKeyCredential(storageAccount, accountKey)
	if err != nil {
		return nil, err
	}

	var store = &azureAccountStore{
		azureStoreBase: azureStoreBase{
			storageAccount: storageAccount,
			container:      container,
			prefix:         prefix,
			args:           args,
			pipeline:       pipeline,
		},
		sasKey: sasKey,
	}

	log.WithFields(log.Fields{
		"storageAccount": storageAccount,
		"container":      container,
		"prefix":         prefix,
	}).Info("constructed new Azure Shared Key storage client")

	return store, nil
}

// SignGet returns a signed URL for GET operations using Shared Key signing
func (a *azureAccountStore) SignGet(fragment pb.Fragment, d time.Duration) (string, error) {
	var blob = a.args.rewritePath(a.prefix, fragment.ContentPath())

	sasQueryParams, err := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		ExpiryTime:    time.Now().UTC().Add(d),
		ContainerName: a.container,
		BlobName:      blob,
		Permissions:   to.Ptr(sas.BlobPermissions{Read: true}).String(),
	}.SignWithSharedKey(a.sasKey)

	if err != nil {
		return "", err
	}

	log.WithFields(log.Fields{
		"storageAccount": a.storageAccount,
		"container":      a.container,
		"blob":           blob,
		"expires":        sasQueryParams.ExpiryTime(),
	}).Debug("Signed get request with shared key")

	return fmt.Sprintf("%s/%s?%s", a.containerURL(), blob, sasQueryParams.Encode()), nil
}
