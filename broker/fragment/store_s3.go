package fragment

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
)

// S3StoreQueryArgs contains fields that are parsed from the query arguments
// of an s3:// fragment store URL.
type S3StoreQueryArgs struct {
	RewriterConfig
	// AWS Profile to extract credentials from the shared credentials file.
	// For details, see:
	//   https://aws.amazon.com/blogs/security/a-new-and-standardized-way-to-manage-credentials-in-the-aws-sdks/
	// If empty, the default credentials are used.
	Profile string
	// Endpoint to connect to S3. If empty, the default S3 service is used.
	Endpoint string
	// ACL applied when persisting new fragments. By default, this is
	// s3.ObjectCannedACLBucketOwnerFullControl.
	ACL string
	// Storage class applied when persisting new fragments. By default,
	// this is s3.ObjectStorageClassStandard.
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

type s3Store struct {
	bucket string
	prefix string
	args   S3StoreQueryArgs
	client *s3.S3
}

func newS3Store(ep *url.URL) (*s3Store, error) {
	var args S3StoreQueryArgs
	if err := parseStoreArgs(ep, &args); err != nil {
		return nil, err
	}
	// Omit leading slash from bucket prefix. Note that FragmentStore already
	// enforces that URL Paths end in '/'.
	var bucket, prefix = ep.Host, ep.Path[1:]

	var awsConfig = aws.NewConfig()
	awsConfig.WithCredentialsChainVerboseErrors(true)

	if args.Region != "" {
		awsConfig.WithRegion(args.Region)
	}

	if args.Endpoint != "" {
		awsConfig.WithEndpoint(args.Endpoint)
		// We must force path style because bucket-named virtual hosts
		// are not compatible with explicit endpoints.
		awsConfig.WithS3ForcePathStyle(true)
	}
	awsConfig.WithHTTPClient(httpClientDisableCompression)

	awsSession, err := session.NewSessionWithOptions(session.Options{
		Config:  *awsConfig,
		Profile: args.Profile,
	})
	if err != nil {
		return nil, fmt.Errorf("constructing S3 session: %s", err)
	}

	creds, err := awsSession.Config.Credentials.Get()
	if err != nil {
		return nil, fmt.Errorf("fetching AWS credentials for profile %q: %s", args.Profile, err)
	}

	// The aws sdk will always just return an error if this Region is not set, even if
	// the Endpoint was provided explicitly. It's important to return an error here
	// in that case, before adding this client to the `clients` map.
	if awsSession.Config.Region == nil || *awsSession.Config.Region == "" {
		return nil, fmt.Errorf("missing AWS region configuration for profile %q", args.Profile)
	}

	log.WithFields(log.Fields{
		"endpoint":     args.Endpoint,
		"profile":      args.Profile,
		"region":       *awsSession.Config.Region,
		"keyID":        creds.AccessKeyID,
		"providerName": creds.ProviderName,
	}).Info("constructed new aws.Session")

	var client = s3.New(awsSession)

	return &s3Store{
		bucket: bucket,
		prefix: prefix,
		args:   args,
		client: client,
	}, nil
}

func (s *s3Store) Provider() string {
	return "s3"
}

func (s *s3Store) SignGet(fragment pb.Fragment, d time.Duration) (string, error) {
	var getObj = s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.args.rewritePath(s.prefix, fragment.ContentPath())),
	}
	var req, _ = s.client.GetObjectRequest(&getObj)
	return req.Presign(d)
}

func (s *s3Store) Exists(ctx context.Context, fragment pb.Fragment) (bool, error) {
	var headObj = s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.args.rewritePath(s.prefix, fragment.ContentPath())),
	}
	if _, err := s.client.HeadObjectWithContext(ctx, &headObj); err == nil {
		return true, nil
	} else if awsErr, ok := err.(awserr.RequestFailure); ok && awsErr.StatusCode() == http.StatusNotFound {
		return false, nil
	} else {
		return false, err
	}
}

func (s *s3Store) Open(ctx context.Context, fragment pb.Fragment) (io.ReadCloser, error) {
	var getObj = s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.args.rewritePath(s.prefix, fragment.ContentPath())),
	}
	var resp *s3.GetObjectOutput
	var err error
	if resp, err = s.client.GetObjectWithContext(ctx, &getObj); err != nil {
		return nil, err
	}
	return resp.Body, err
}

func (s *s3Store) Persist(ctx context.Context, spool Spool) error {
	var putObj = s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.args.rewritePath(s.prefix, spool.ContentPath())),
	}

	if s.args.ACL != "" {
		putObj.ACL = aws.String(s.args.ACL)
	}
	if s.args.StorageClass != "" {
		putObj.StorageClass = aws.String(s.args.StorageClass)
	}
	if s.args.SSE != "" {
		putObj.ServerSideEncryption = aws.String(s.args.SSE)
	}
	if s.args.SSEKMSKeyId != "" {
		putObj.SSEKMSKeyId = aws.String(s.args.SSEKMSKeyId)
	}
	if spool.CompressionCodec == pb.CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION {
		putObj.ContentEncoding = aws.String("gzip")
	}
	if spool.CompressionCodec != pb.CompressionCodec_NONE {
		putObj.Body = io.NewSectionReader(spool.compressedFile, 0, spool.compressedLength)
	} else {
		putObj.Body = io.NewSectionReader(spool.File, 0, spool.ContentLength())
	}
	_, err := s.client.PutObjectWithContext(ctx, &putObj)
	return err
}

func (s *s3Store) List(ctx context.Context, journal pb.Journal, callback func(pb.Fragment)) error {
	var q = s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(s.args.rewritePath(s.prefix, journal.String()) + "/"),
	}
	return s.client.ListObjectsV2PagesWithContext(ctx, &q, func(objs *s3.ListObjectsV2Output, _ bool) bool {
		for _, obj := range objs.Contents {
			if strings.HasSuffix(*obj.Key, "/") {
				// Ignore directory-like objects, usually created by mounting buckets with a FUSE driver.
			} else if frag, err := pb.ParseFragmentFromRelativePath(journal, (*obj.Key)[len(*q.Prefix):]); err != nil {
				log.WithFields(log.Fields{"bucket": s.bucket, "key": *obj.Key, "err": err}).Warning("parsing fragment")
			} else if *obj.Size == 0 && frag.ContentLength() > 0 {
				log.WithFields(log.Fields{"obj": obj}).Warning("zero-length fragment")
			} else {
				frag.ModTime = obj.LastModified.Unix()
				callback(frag)
			}
		}
		return true
	})
}

func (s *s3Store) Remove(ctx context.Context, fragment pb.Fragment) error {
	var deleteObj = s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.args.rewritePath(s.prefix, fragment.ContentPath())),
	}

	_, err := s.client.DeleteObjectWithContext(ctx, &deleteObj)
	return err
}

func (s *s3Store) IsAuthError(err error) bool {
	if awsErr, ok := err.(awserr.Error); ok {
		switch awsErr.Code() {
		case s3.ErrCodeNoSuchBucket:
			return true
		case s3ErrCodeAccessDenied:
			return true
		}
	}
	if awsErr, ok := err.(awserr.RequestFailure); ok {
		if awsErr.StatusCode() == http.StatusForbidden {
			return true
		}
	}
	return false
}

const (
	// AWS S3 error codes not defined as constants in the SDK
	s3ErrCodeAccessDenied = "AccessDenied"
)
