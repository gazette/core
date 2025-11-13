package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/schema"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/codecs"
	pb "go.gazette.dev/core/broker/protocol"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Reader adapts a Read RPC to the io.Reader interface. The first byte read from
// the Reader initiates the RPC, and subsequent reads stream from it.
//
// If DoNotProxy is true then the broker may close the RPC after sending a signed
// Fragment URL, and Reader will directly open the Fragment (decompressing if needed),
// seek to the requested offset, and read its content.
//
// Reader returns EOF if:
//   - The broker closes the RPC, eg because its assignment has change or it's shutting down.
//   - The requested EndOffset has been read through.
//   - A Fragment being read by the Reader reaches EOF.
//
// If Block is true, Read may block indefinitely. Otherwise, ErrOffsetNotYetAvailable
// is returned upon reaching the journal write head.
//
// A Reader is invalidated by its first returned error, with the exception of
// ErrOffsetJump: this error is returned to notify the client that the next Journal
// offset to be Read is not the offset that was requested (eg, because a portion of
// the Journal was deleted), but the Reader is prepared to continue at the updated,
// strictly larger offset.
type Reader struct {
	Request  pb.ReadRequest  // ReadRequest of the Reader.
	Response pb.ReadResponse // Most recent ReadResponse from broker.

	ctx     context.Context
	client  pb.RoutedJournalClient // Client against which Read is dispatched.
	counter prometheus.Counter     // Counter of read bytes.
	stream  pb.Journal_ReadClient  // Server stream.
	direct  io.ReadCloser          // Directly opened Fragment URL.
}

// NewReader returns an initialized Reader of the given ReadRequest.
func NewReader(ctx context.Context, client pb.RoutedJournalClient, req pb.ReadRequest) *Reader {
	var r = &Reader{
		Request: req,
		ctx:     ctx,
		client:  client,
		counter: readBytes.WithLabelValues(req.Journal.String()),
	}
	return r
}

// Read from the journal. If this is the first Read of the Reader, a Read RPC is started.
func (r *Reader) Read(p []byte) (n int, err error) {
	// If we have an open direct reader of a persisted fragment, delegate to it.
	if r.direct != nil {

		// Return EOF and close |direct| on reading through EndOffset.
		if r.Request.EndOffset != 0 {
			if remain := r.Request.EndOffset - r.Request.Offset; remain == 0 {
				if err = r.direct.Close(); err == nil {
					n, err = 0, io.EOF
				}
				return
			} else if int64(len(p)) > remain {
				p = p[:remain]
			}
		}

		if n, err = r.direct.Read(p); err != nil {
			_ = r.direct.Close()
		}
		r.Request.Offset += int64(n)
		r.counter.Add(float64(n))
		return
	}

	// Is there remaining content in the last ReadResponse?
	if l, d := len(r.Response.Content), int(r.Request.Offset-r.Response.Offset); l != 0 && l > d {
		n = copy(p, r.Response.Content[d:])
		r.Request.Offset += int64(n)
		r.counter.Add(float64(n))
		return
	}

	// Lazy initialization: begin the Read RPC.
	if r.stream == nil {
		var ctx context.Context

		// Prefer a prior response header route, and fall back to the route cache.
		if r.Response.Header != nil {
			ctx = pb.WithDispatchRoute(r.ctx, r.Response.Header.Route, pb.ProcessSpec_ID{})
		} else {
			ctx = pb.WithDispatchItemRoute(r.ctx, r.client, r.Request.Journal.String(), false)
		}
		// Clear last response of previous stream.
		r.Response = pb.ReadResponse{}

		if r.stream, err = r.client.Read(ctx, &r.Request); err == nil {
			n, err = r.Read(p) // Recurse to attempt read against opened |r.stream|.
		} else {
			err = mapGRPCCtxErr(r.ctx, err)
		}
		return
	}

	var previous = r.Response

	// Read and Validate the next frame.
	if err = r.stream.RecvMsg(&r.Response); err == nil {
		if err = r.Response.Validate(); err != nil {
			err = pb.ExtendContext(err, "ReadResponse")
		} else if r.Response.Status == pb.Status_OK && r.Response.Offset < r.Request.Offset {
			err = pb.NewValidationError("invalid ReadResponse offset (%d; expected >= %d)",
				r.Response.Offset, r.Request.Offset) // Violation of Read API contract.
		}
	} else {
		err = mapGRPCCtxErr(r.ctx, err)
	}

	// A note on resource leaks: an invariant of Read is that in invocations where
	// the returned error != nil, an error has also been read from |r.stream|,
	// implying that the gRPC stream has been torn down. The exception is if
	// response validation fails, which indicates a client / server API version
	// incompatibility and cannot happen in normal operation.

	if err == nil {
		// If a Header was sent, advise of its advertised journal Route.
		if r.Response.Header != nil {
			r.client.UpdateRoute(r.Request.Journal.String(), &r.Response.Header.Route)
		}

		if r.Request.Offset < r.Response.Offset {
			// Offset jumps are uncommon, but possible if fragments were removed,
			// or if the requested offset was -1.
			r.Request.Offset = r.Response.Offset
			err = ErrOffsetJump
		}

		if r.Response.Status == pb.Status_OK {
			if r.Response.Content != nil {
				// This is a content chunk. Preserve it's associated fragment
				// metadata for the benefit of clients who want to inspect it.
				if r.Response.Fragment != nil {
					panic("unexpected fragment of content chunk")
				}
				r.Response.Fragment = previous.Fragment
				r.Response.WriteHead = previous.WriteHead
			}
			// Return empty read, to allow inspection of the updated |r.Response|.
		} else {
			// The broker will send a stream closure following a !OK status.
			// Recurse to read that closure, and _then_ return a final error.
			n, err = r.Read(p)
		}
		return

	} else if err != io.EOF {
		// We read an error _other_ than a graceful tear-down of the stream.
		return
	}

	// We read a graceful stream closure (err == io.EOF).

	// If the frame preceding EOF provided a fragment URL, open it directly.
	if !r.Request.MetadataOnly && r.Response.Status == pb.Status_OK && r.Response.FragmentUrl != "" {
		if SkipSignedURLs {
			fragURL := r.Response.Fragment.BackingStore.URL()
			if fragURL.Scheme != "gs" && fragURL.Scheme != "s3" {
				return 0, fmt.Errorf("SkipSignedURL unsupported scheme: %s", fragURL.Scheme)
			}
			if r.direct, err = OpenUnsignedFragmentURL(r.ctx, fragURL.Scheme, *r.Response.Fragment,
				r.Request.Offset, fragURL); err == nil {
				n, err = r.Read(p) // Recurse to attempt read against opened |r.direct|.
			}
		} else {
			if r.direct, err = OpenFragmentURL(r.ctx, *r.Response.Fragment,
				r.Request.Offset, r.Response.FragmentUrl); err == nil {
				n, err = r.Read(p) // Recurse to attempt read against opened |r.direct|.
			}
		}
		return
	}

	// Otherwise, map Status of the preceding frame into a more specific error.
	switch r.Response.Status {
	case pb.Status_OK:
		if err != io.EOF {
			panic(err.Error()) // Status_OK implies graceful stream closure.
		}
	case pb.Status_JOURNAL_NOT_FOUND:
		err = ErrJournalNotFound
	case pb.Status_NOT_JOURNAL_BROKER:
		err = ErrNotJournalBroker
	case pb.Status_INSUFFICIENT_JOURNAL_BROKERS:
		err = ErrInsufficientJournalBrokers
	case pb.Status_OFFSET_NOT_YET_AVAILABLE:
		err = ErrOffsetNotYetAvailable
	case pb.Status_SUSPENDED:
		err = ErrSuspended
	default:
		err = errors.New(r.Response.Status.String())
	}
	return
}

// AdjustedOffset returns the current journal offset adjusted for content read
// by the bufio.Reader (which must wrap this Reader), which has not yet been
// consumed from the bufio.Reader's buffer.
func (r *Reader) AdjustedOffset(br *bufio.Reader) int64 {
	return r.Request.Offset - int64(br.Buffered())
}

// Seek provides a limited form of seeking support. Specifically, if:
//   - A Fragment URL is being directly read, and
//   - The Seek offset is ahead of the current Reader offset, and
//   - The Fragment also covers the desired Seek offset
//
// Then a seek is performed by reading and discarding to the seeked offset.
// Seek will otherwise return ErrSeekRequiresNewReader.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		// |offset| is already absolute.
	case io.SeekCurrent:
		offset = r.Request.Offset + offset
	case io.SeekEnd:
		return r.Request.Offset, errors.New("io.SeekEnd whence is not supported")
	default:
		panic("invalid whence")
	}

	if r.direct == nil || offset < r.Request.Offset || offset >= r.Response.Fragment.End {
		return r.Request.Offset, ErrSeekRequiresNewReader
	}

	var _, err = io.CopyN(ioutil.Discard, r, offset-r.Request.Offset)
	return r.Request.Offset, err
}

// OpenFragmentURL directly opens the Fragment, which must be available at the
// given URL, and returns a *FragmentReader which has been pre-seeked to the
// given offset.
func OpenFragmentURL(ctx context.Context, fragment pb.Fragment, offset int64, url string) (*FragmentReader, error) {
	var req, err = http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	if fragment.CompressionCodec == pb.CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION {
		// Require that the server send us un-encoded content, offloading
		// decompression onto the storage API. Go's standard `gzip` package is slow,
		// and we also see a parallelism benefit by offloading decompression work
		// onto the cloud storage system.
		req.Header.Set("Accept-Encoding", "identity")
	} else if fragment.CompressionCodec == pb.CompressionCodec_GZIP {
		// Explicitly request gzip. Doing so disables the http client's transparent
		// handling for gzip decompression if the Fragment happened to be written with
		// "Content-Encoding: gzip", and it instead directly surfaces the compressed
		// bytes to us.
		req.Header.Set("Accept-Encoding", "gzip")
	}

	resp, err := httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	} else if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("!OK fetching (%s, %q)", resp.Status, url)
	}

	// Technically the store _must_ decompress in response to honor our
	// Accept-Encoding header, but some implementations (eg, Minio) don't.
	if fragment.CompressionCodec == pb.CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION &&
		resp.Header.Get("Content-Encoding") == "gzip" {

		fragment.CompressionCodec = pb.CompressionCodec_GZIP // Decompress client-side.
	}

	// Record metrics related to opening the fragment.
	var labels = fragmentLabels(fragment)
	fragmentOpen.With(labels).Inc()
	fragmentOpenBytes.With(labels).Add(float64(fragment.End - fragment.Begin))
	if resp.ContentLength != -1 {
		fragmentOpenContentLength.With(labels).Add(float64(resp.ContentLength))
	}

	return NewFragmentReader(resp.Body, fragment, offset)
}

func OpenUnsignedFragmentURL(ctx context.Context, scheme string, fragment pb.Fragment, offset int64, url *url.URL) (*FragmentReader, error) {
	var (
		rdr io.ReadCloser
		err error
	)

	if scheme == "gs" {
		if rdr, err = gcsAccess.open(ctx, url, fragment); err != nil {
			return nil, err
		}
	} else if scheme == "s3" {
		if rdr, err = s3Access.open(ctx, url, fragment); err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("unsupported scheme: %s", scheme)
	}

	// Record metrics related to opening the fragment.
	var labels = fragmentLabels(fragment)
	fragmentOpen.With(labels).Inc()
	fragmentOpenBytes.With(labels).Add(float64(fragment.End - fragment.Begin))

	return NewFragmentReader(rdr, fragment, offset)
}

// NewFragmentReader wraps a io.ReadCloser of raw Fragment bytes with a
// returned *FragmentReader which has been pre-seeked to the given offset.
func NewFragmentReader(rc io.ReadCloser, fragment pb.Fragment, offset int64) (*FragmentReader, error) {
	var decomp, err = codecs.NewCodecReader(rc, fragment.CompressionCodec)
	if err != nil {
		_ = rc.Close()
		return nil, err
	}

	var fr = &FragmentReader{
		decomp:   decomp,
		raw:      rc,
		Fragment: fragment,
		Offset:   fragment.Begin,
		counter:  discardFragmentBytes.With(fragmentLabels(fragment)),
	}

	// Attempt to seek to |offset| within the fragment.
	var delta = offset - fragment.Begin
	if _, err = io.CopyN(ioutil.Discard, fr, delta); err != nil {
		_ = fr.Close()
		return nil, err
	}
	// We've finished discarding required bytes.
	fr.counter = readFragmentBytes.With(fragmentLabels(fragment))

	return fr, nil
}

// FragmentReader directly reads from an opened Fragment file.
type FragmentReader struct {
	pb.Fragment       // Fragment being read.
	Offset      int64 // Next journal offset to be read, in range [Begin, End).

	decomp  io.ReadCloser
	raw     io.ReadCloser
	counter prometheus.Counter
}

// Read returns the next bytes of decompressed Fragment content. When Read
// returns, Offset has been updated to reflect the next byte to be read.
// Read returns EOF only if the underlying Reader returns EOF at precisely
// Offset == Fragment.End. If the underlying Reader is too short,
// io.ErrUnexpectedEOF is returned. If it's too long, ErrDidNotReadExpectedEOF
// is returned.
func (fr *FragmentReader) Read(p []byte) (n int, err error) {
	n, err = fr.decomp.Read(p)
	fr.Offset += int64(n)

	if fr.Offset > fr.Fragment.End {
		// Did we somehow read beyond Fragment.End?
		n -= int(fr.Offset - fr.Fragment.End)
		err = ErrDidNotReadExpectedEOF
	} else if err == io.EOF && fr.Offset != fr.Fragment.End {
		// Did we read EOF before the reaching Fragment.End?
		log.WithFields(log.Fields{"stack": string(debug.Stack())}).Warn("unexpected EOF being set #5")
		err = io.ErrUnexpectedEOF
	}
	fr.counter.Add(float64(n))
	return
}

// Close closes the underlying ReadCloser and associated
// decompressor (if any).
func (fr *FragmentReader) Close() error {
	var errA, errB = fr.decomp.Close(), fr.raw.Close()
	if errA != nil {
		return errA
	}
	return errB
}

// InstallFileTransport registers a file:// protocol handler at the given root
// with the http.Client used by OpenFragmentURL. It's used in testing contexts,
// and is also appropriate when brokers share a common NAS file store to which
// fragments are persisted, and to which this client also has access. The
// returned cleanup function removes the handler and restores the prior http.Client.
//
//	const root = "/mnt/shared-nas-array/path/to/fragment-root"
//	defer client.InstallFileTransport(root)()
//
//	var rr = NewRetryReader(ctx, client, protocol.ReadRequest{
//	    Journal: "a/journal/with/nas/fragment/store",
//	    DoNotProxy: true,
//	})
//	// rr.Read will read Fragments directly from NAS.
func InstallFileTransport(root string) (remove func()) {
	var transport = httpClient.Transport.(*http.Transport).Clone()
	transport.RegisterProtocol("file", http.NewFileTransport(http.Dir(root)))

	var prevClient = httpClient
	httpClient = &http.Client{Transport: transport}

	return func() { httpClient = prevClient }
}

// mapGRPCCtxErr returns ctx.Err() iff |err| represents a gRPC error with a
// status code matching ctx.Err(). Otherwise, it returns |err| unmodified.
// In other words, this routine "unwraps" gRPC errors which have their root cause
// in a local Context error, instead mapping to the common context package errors.
func mapGRPCCtxErr(ctx context.Context, err error) error {
	if ctx.Err() == context.DeadlineExceeded && status.Code(err) == codes.DeadlineExceeded {
		return ctx.Err()
	}
	if ctx.Err() == context.Canceled && status.Code(err) == codes.Canceled {
		return ctx.Err()
	}
	return err
}

func fragmentLabels(fragment pb.Fragment) prometheus.Labels {
	return prometheus.Labels{
		"journal": fragment.Journal.String(),
		"codec":   fragment.CompressionCodec.String(),
	}
}

// newHttpClient returns an http client for readers to use for fetching fragments.
// It disables http2 because we've observed some rather horrific behavior from http2
// lately. When there's an error with the underlying transport, the http2 client can still
// use it for additional streams, creating the potential for connection failures to cause
// more widespread errors for other requests to the same host. Falling back to http 1.1
// is intended to be a short term workaround.
func newHttpClient() *http.Client {
	// The Go docs on disabling http2 are wrong. See: https://github.com/golang/go/issues/39302
	return &http.Client{
		Transport: &http.Transport{
			TLSNextProto:      make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
			TLSClientConfig:   &tls.Config{},
			ForceAttemptHTTP2: false,
			// Defaults below are taken from http.DefaultTransport
			Proxy:                 http.ProxyFromEnvironment,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
}

var (
	// Map common broker error statuses into named errors.
	ErrInsufficientJournalBrokers = errors.New(pb.Status_INSUFFICIENT_JOURNAL_BROKERS.String())
	ErrJournalNotFound            = errors.New(pb.Status_JOURNAL_NOT_FOUND.String())
	ErrNoJournalPrimaryBroker     = errors.New(pb.Status_NO_JOURNAL_PRIMARY_BROKER.String())
	ErrNotJournalBroker           = errors.New(pb.Status_NOT_JOURNAL_BROKER.String())
	ErrNotJournalPrimaryBroker    = errors.New(pb.Status_NOT_JOURNAL_PRIMARY_BROKER.String())
	ErrOffsetNotYetAvailable      = errors.New(pb.Status_OFFSET_NOT_YET_AVAILABLE.String())
	ErrRegisterMismatch           = errors.New(pb.Status_REGISTER_MISMATCH.String())
	ErrSuspended                  = errors.New(pb.Status_SUSPENDED.String())
	ErrWrongAppendOffset          = errors.New(pb.Status_WRONG_APPEND_OFFSET.String())

	// ErrOffsetJump is returned by Reader.Read to indicate that the next byte
	// available to be read is at a larger offset than that requested (eg,
	// because a span of the Journal has been deleted). The Reader's ReadResponse
	// should be inspected by the caller, and Read may be invoked again to continue.
	ErrOffsetJump = errors.New("offset jump")
	// ErrSeekRequiresNewReader is returned by Reader.Seek if it is unable to
	// satisfy the requested Seek. A new Reader should be started instead.
	ErrSeekRequiresNewReader = errors.New("seek offset requires new Reader")
	// ErrDidNotReadExpectedEOF is returned by FragmentReader.Read if the
	// underlying file did not return EOF at the expected Fragment End offset.
	ErrDidNotReadExpectedEOF = errors.New("did not read EOF at expected Fragment.End")

	// httpClient is the http.Client used by OpenFragmentURL
	httpClient = newHttpClient()
)

// ARIZE specific code to end of file.
//
// To support unsigned URLs we need to be able to deal with buckets directly as a consumer and not via the signed URL.
// In OpenUnsignedFragmentURL we need to be able to open the fragment directly from the bucket. It would have
// been nice to use the backend interface in stores.go which the broker uses to access buckets. Unfortunately
// stores_test.go, which is in broker/fragment, imports broker/client so we cannot import broker/fragment here
// to avoid a cycle. Instead we will repeat a subset of store_gcs.go and store_s3.go.
// This makes the use support of unsigned URLs very gcs and s3 specific.

var (
	SkipSignedURLs = false
	gcsAccess      = &gcsBackend{}
	s3Access       = newS3Backend()
	S3Creds        *credentials.Credentials
)

// ////////////
// GCS backend for use with consumers and unsigned URLs.
// ////////////
type gcsBackend struct {
	client   *storage.Client
	clientMu sync.Mutex
}

// Arize Open routine for use with consumers and signed URLs.
func (s *gcsBackend) open(ctx context.Context, ep *url.URL, fragment pb.Fragment) (io.ReadCloser, error) {
	cfg, gClient, err := s.gcsClient(ep)
	if err != nil {
		return nil, err
	}
	return gClient.Bucket(cfg.bucket).Object(cfg.rewritePath(cfg.prefix, fragment.ContentPath())).NewReader(ctx)
}

// to help identify when JSON credentials are an external account used by workload identity
type credentialsFile struct {
	Type string `json:"type"`
}

func (s *gcsBackend) gcsClient(ep *url.URL) (cfg GSStoreConfig, client *storage.Client, err error) {
	var conf *jwt.Config

	if err = parseStoreArgs(ep, &cfg); err != nil {
		return
	}
	// Omit leading slash from bucket prefix. Note that FragmentStore already
	// enforces that URL Paths end in '/'.
	cfg.bucket, cfg.prefix = ep.Host, ep.Path[1:]

	s.clientMu.Lock()
	defer s.clientMu.Unlock()

	if s.client != nil {
		client = s.client
		return
	}
	var ctx = context.Background()

	creds, err := google.FindDefaultCredentials(ctx, storage.ScopeFullControl)
	if err != nil {
		return
	}

	// best effort to determine if JWT credentials are for external account
	externalAccount := false
	if creds.JSON != nil {
		var f credentialsFile
		if err := json.Unmarshal(creds.JSON, &f); err == nil {
			externalAccount = f.Type == "external_account"
		}
	}

	if creds.JSON != nil && !externalAccount {
		conf, err = google.JWTConfigFromJSON(creds.JSON, storage.ScopeFullControl)
		if err != nil {
			return
		}
		client, err = storage.NewClient(ctx, option.WithTokenSource(conf.TokenSource(ctx)))
		if err != nil {
			return
		}
		s.client = client

		log.WithFields(log.Fields{
			"ProjectID":      creds.ProjectID,
			"GoogleAccessID": conf.Email,
			"PrivateKeyID":   conf.PrivateKeyID,
			"Subject":        conf.Subject,
			"Scopes":         conf.Scopes,
		}).Info("reader constructed new GCS client")
	} else {
		// Possible to use GCS without a service account (e.g. with a GCE instance and workload identity).
		client, err = storage.NewClient(ctx, option.WithTokenSource(creds.TokenSource))
		if err != nil {
			return
		}

		// workload identity approach which SignGet() method accepts if you have
		// "iam.serviceAccounts.signBlob" permissions against your service account.
		s.client = client

		log.WithFields(log.Fields{
			"ProjectID": creds.ProjectID,
		}).Info("reader constructed new GCS client without JWT")
	}

	return
}

type GSStoreConfig struct {
	bucket string
	prefix string

	RewriterConfig
}

// ////////////
// S3 backend for use with consumers and unsigned URLs.
// ////////////
type s3Backend struct {
	clients   map[[3]string]*s3.S3
	clientsMu sync.Mutex
}

func newS3Backend() *s3Backend {
	return &s3Backend{
		clients: make(map[[3]string]*s3.S3),
	}
}

func (s *s3Backend) open(ctx context.Context, ep *url.URL, fragment pb.Fragment) (io.ReadCloser, error) {
	cfg, client, err := s.s3Client(ep)
	if err != nil {
		return nil, err
	}

	var getObj = s3.GetObjectInput{
		Bucket: aws.String(cfg.bucket),
		Key:    aws.String(cfg.rewritePath(cfg.prefix, fragment.ContentPath())),
	}
	var resp *s3.GetObjectOutput
	if resp, err = client.GetObjectWithContext(ctx, &getObj); err != nil {
		return nil, err
	}
	return resp.Body, err
}

func (s *s3Backend) s3Client(ep *url.URL) (cfg S3StoreConfig, client *s3.S3, err error) {
	if err = parseStoreArgs(ep, &cfg); err != nil {
		return
	}
	// Omit leading slash from bucket prefix. Note that FragmentStore already
	// enforces that URL Paths end in '/'.
	cfg.bucket, cfg.prefix = ep.Host, ep.Path[1:]

	defer s.clientsMu.Unlock()
	s.clientsMu.Lock()

	var key = [3]string{cfg.Endpoint, cfg.Profile, cfg.Region}
	if client = s.clients[key]; client != nil {
		return
	}

	var awsConfig = aws.NewConfig()
	awsConfig.WithCredentialsChainVerboseErrors(true)
	awsConfig.WithCredentials(S3Creds)

	if cfg.Region != "" {
		awsConfig.WithRegion(cfg.Region)
	}

	if cfg.Endpoint != "" {
		awsConfig.WithEndpoint(cfg.Endpoint)
		// We must force path style because bucket-named virtual hosts
		// are not compatible with explicit endpoints.
		awsConfig.WithS3ForcePathStyle(true)
	} else {
		// Real S3. Override the default http.Transport's behavior of inserting
		// "Accept-Encoding: gzip" and transparently decompressing client-side.
		awsConfig.WithHTTPClient(&http.Client{

			Transport: &http.Transport{DisableCompression: true},
		})
	}

	awsSession, err := session.NewSessionWithOptions(session.Options{
		Config: *awsConfig,
	})
	if err != nil {
		err = fmt.Errorf("constructing S3 session: %s", err)
		return
	}

	creds, err := awsSession.Config.Credentials.Get()
	if err != nil {
		err = fmt.Errorf("fetching AWS credentials: %s", err)
		return
	}

	// The aws sdk will always just return an error if this Region is not set, even if
	// the Endpoint was provided explicitly. It's important to return an error here
	// in that case, before adding this client to the `clients` map.
	if awsSession.Config.Region == nil || *awsSession.Config.Region == "" {
		err = fmt.Errorf("missing AWS region configuration for profile %q", cfg.Profile)
		return
	}

	log.WithFields(log.Fields{
		"endpoint":     cfg.Endpoint,
		"profile":      cfg.Profile,
		"region":       *awsSession.Config.Region,
		"keyID":        creds.AccessKeyID,
		"providerName": creds.ProviderName,
	}).Info("constructed new aws.Session")

	client = s3.New(awsSession, awsConfig)
	s.clients[key] = client

	return
}

type S3StoreConfig struct {
	bucket string
	prefix string

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

// ////////////
// Helper functions for parsing store URLs and rewriting paths.
// ////////////
type RewriterConfig struct {
	// Find is the string to replace in the unmodified journal name.
	Find string
	// Replace is the string with which Find is replaced in the constructed store path.
	Replace string
}

func (cfg RewriterConfig) rewritePath(s, j string) string {
	if cfg.Find == "" {
		return s + j
	}
	return s + strings.Replace(j, cfg.Find, cfg.Replace, 1)
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
