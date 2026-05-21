package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/gorilla/schema"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/stores"
	"go.gazette.dev/core/broker/stores/common"
)

// StoreQueryArgs contains fields that are parsed from the query arguments
// of an s3:// fragment store URL.
type StoreQueryArgs struct {
	common.RewriterConfig
	// AWS Profile to extract credentials from the shared credentials file.
	// For details, see:
	//   https://aws.amazon.com/blogs/security/a-new-and-standardized-way-to-manage-credentials-in-the-aws-sdks/
	// If empty, the default credentials are used.
	Profile string
	// Endpoint to connect to S3. If empty, the default S3 service is used.
	Endpoint string
	// ACL applied when persisting new fragments. By default, this is
	// s3types.ObjectCannedACLBucketOwnerFullControl.
	ACL string
	// Storage class applied when persisting new fragments. By default,
	// this is s3types.ObjectStorageClassStandard.
	StorageClass string
	// SSE is the server-side encryption type to be applied (eg, "AES256").
	// By default, encryption is not used.
	SSE string
	// SSEKMSKeyId specifies the ID for the AWS KMS symmetric customer managed key
	// By default, not used.
	SSEKMSKeyId string
	// Region is the region for the bucket. If empty, the region is determined
	// from `Profile` or the default credentials.
	Region string
}

type store struct {
	bucket string
	prefix string
	region string
	args   StoreQueryArgs
	client *s3.Client
}

// New creates a new S3 Store from the provided URL.
func New(ep *url.URL) (stores.Store, error) {
	var args StoreQueryArgs
	if err := parseStoreArgs(ep, &args); err != nil {
		return nil, err
	}
	// Omit leading slash from bucket prefix. Note that FragmentStore already
	// enforces that URL Paths end in '/'.
	var bucket, prefix = ep.Host, ep.Path[1:]

	var loadOpts []func(*config.LoadOptions) error
	if args.Profile != "" {
		loadOpts = append(loadOpts, config.WithSharedConfigProfile(args.Profile))
	}
	if args.Region != "" {
		loadOpts = append(loadOpts, config.WithRegion(args.Region))
	}
	if args.Endpoint == "" {
		// Real S3. Override the default http.Transport's behavior of inserting
		// "Accept-Encoding: gzip" and transparently decompressing client-side.
		loadOpts = append(loadOpts, config.WithHTTPClient(&http.Client{
			Transport: &http.Transport{DisableCompression: true},
		}))
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("constructing AWS config: %w", err)
	}

	creds, err := cfg.Credentials.Retrieve(context.Background())
	if err != nil {
		return nil, fmt.Errorf("fetching AWS credentials for profile %q: %w", args.Profile, err)
	}

	// The aws sdk will always just return an error if this Region is not set, even if
	// the Endpoint was provided explicitly. It's important to fail-fast in this case.
	if cfg.Region == "" {
		return nil, fmt.Errorf("missing AWS region configuration for profile %q", args.Profile)
	}

	log.WithFields(log.Fields{
		"endpoint":     args.Endpoint,
		"profile":      args.Profile,
		"region":       cfg.Region,
		"keyID":        creds.AccessKeyID,
		"providerName": creds.Source,
	}).Info("constructed new AWS config")

	// arize fix for sts:AssumeRoleWithWebIdentity, #16048
	var client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		if args.Endpoint != "" {
			o.BaseEndpoint = aws.String(args.Endpoint)
			// We must force path style because bucket-named virtual hosts
			// are not compatible with explicit endpoints.
			o.UsePathStyle = true
		}
		// Leave o.UseARNRegion = true and o.DisableMultiRegionAccessPoints = false
		// at v2 defaults so bucket values that are S3 access-point or MRAP
		// ARNs resolve to their correct endpoint and use SigV4A signing.
	})

	return &store{
		bucket: bucket,
		prefix: prefix,
		region: cfg.Region,
		args:   args,
		client: client,
	}, nil
}

func (s *store) SignGet(path string, d time.Duration) (string, error) {
	var key = s.args.RewritePath(s.prefix, path)

	if stores.DisableSignedUrls {
		// S3 access-point and MRAP ARNs (resolved by the SDK to a special
		// endpoint and requiring SigV4A signing) have no valid unsigned URL
		// form. Fail loudly rather than producing a URL that will 403 at
		// fetch time.
		if strings.HasPrefix(s.bucket, "arn:") &&
			strings.Contains(s.bucket, ":s3") &&
			strings.Contains(s.bucket, ":accesspoint/") {
			return "", fmt.Errorf(
				"broker.disable-signed-urls is incompatible with S3 access point bucket %q",
				s.bucket)
		}
		if s.args.Endpoint != "" {
			// Path-style URL matches what the v1 SDK produced under
			// S3ForcePathStyle and what callers with an explicit endpoint expect.
			return fmt.Sprintf("%s/%s/%s",
				strings.TrimRight(s.args.Endpoint, "/"), s.bucket, key), nil
		}
		// Virtual-host style for real S3 with no explicit endpoint.
		return fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s",
			s.bucket, s.region, key), nil
	}

	var getObj = s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	var presigner = s3.NewPresignClient(s.client)
	out, err := presigner.PresignGetObject(context.Background(), &getObj, s3.WithPresignExpires(d))
	if err != nil {
		return "", err
	}
	return out.URL, nil
}

func (s *store) Exists(ctx context.Context, path string) (bool, error) {
	var headObj = s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.args.RewritePath(s.prefix, path)),
	}
	if _, err := s.client.HeadObject(ctx, &headObj); err == nil {
		return true, nil
	} else {
		var respErr *awshttp.ResponseError
		if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusNotFound {
			return false, nil
		}
		return false, err
	}
}

func (s *store) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	var getObj = s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.args.RewritePath(s.prefix, path)),
	}
	var resp, err = s.client.GetObject(ctx, &getObj)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (s *store) Put(ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
	// S3 SDK requires io.ReadSeeker, so we use io.NewSectionReader to adapt io.ReaderAt
	var putObj = s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.args.RewritePath(s.prefix, path)),
		Body:   io.NewSectionReader(content, 0, contentLength),
	}

	if s.args.ACL != "" {
		putObj.ACL = s3types.ObjectCannedACL(s.args.ACL)
	}
	if s.args.StorageClass != "" {
		putObj.StorageClass = s3types.StorageClass(s.args.StorageClass)
	}
	if s.args.SSE != "" {
		putObj.ServerSideEncryption = s3types.ServerSideEncryption(s.args.SSE)
	}
	if s.args.SSEKMSKeyId != "" {
		putObj.SSEKMSKeyId = aws.String(s.args.SSEKMSKeyId)
	}
	if contentEncoding != "" {
		putObj.ContentEncoding = aws.String(contentEncoding)
	}

	_, err := s.client.PutObject(ctx, &putObj)
	return err
}

func (s *store) List(ctx context.Context, prefix string, callback func(path string, modTime time.Time) error) error {
	prefix = s.args.RewritePath(s.prefix, prefix)
	var q = s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	}
	var paginator = s3.NewListObjectsV2Paginator(s.client, &q)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}
		for _, obj := range page.Contents {
			if strings.HasSuffix(*obj.Key, "/") {
				continue // Ignore directory-like objects
			}
			// Return path relative to the listing prefix
			var relPath = strings.TrimPrefix(*obj.Key, prefix)
			if err := callback(relPath, *obj.LastModified); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *store) Remove(ctx context.Context, path string) error {
	var deleteObj = s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.args.RewritePath(s.prefix, path)),
	}

	_, err := s.client.DeleteObject(ctx, &deleteObj)
	return err
}

func (s *store) IsAuthError(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchBucket":
			return true
		case s3ErrCodeAccessDenied:
			return true
		}
	}
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusForbidden {
		return true
	}
	return false
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

const (
	// AWS S3 error codes not defined as constants in the SDK
	s3ErrCodeAccessDenied = "AccessDenied"
)
