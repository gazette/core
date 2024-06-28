package protocol

import (
	"context"
	"math"
	"time"

	"github.com/golang-jwt/jwt/v5"
	grpc "google.golang.org/grpc"
)

// Capability is a bit-mask of authorized capabilities to a resource.
type Capability uint32

const (
	Capability_LIST      Capability = 1 << 1
	Capability_APPLY     Capability = 1 << 2
	Capability_READ      Capability = 1 << 3
	Capability_APPEND    Capability = 1 << 4
	Capability_REPLICATE Capability = 1 << 5

	// Applications may use Capability bits starting at 1 << 16.

	Capability_ALL Capability = math.MaxUint32
)

// Claims reflect the scope of an authorization. They grant the client the
// indicated Capability against resources matched by the corresponding
// Selector.
type Claims struct {
	Capability Capability    `json:"cap"`
	Selector   LabelSelector `json:"sel"`
	jwt.RegisteredClaims
}

// WithClaims attaches desired Claims to a Context.
// The attached claims will be used by AuthJournalClient to obtain Authorization tokens.
func WithClaims(ctx context.Context, claims Claims) context.Context {
	return context.WithValue(ctx, claimsCtxKey{}, claims)
}

// GetClaims retrieves attached Claims from a Context.
func GetClaims(ctx context.Context) (Claims, bool) {
	var claims, ok = ctx.Value(claimsCtxKey{}).(Claims)
	return claims, ok
}

// Authorizer returns a Context which incorporates a gRPC Authorization for the given Claims.
// It could do so by directly signing the claims with a pre-shared key,
// or by requesting a signature from a separate authorization service.
type Authorizer interface {
	Authorize(ctx context.Context, claims Claims, exp time.Duration) (context.Context, error)
}

// Verifier verifies an Authorization token, returning its validated Claims.
type Verifier interface {
	Verify(ctx context.Context, require Capability) (context.Context, context.CancelFunc, Claims, error)
}

// NewAuthJournalClient returns a JournalClient which uses the Authorizer
// to obtain and attach an Authorization bearer token to every issued request.
func NewAuthJournalClient(jc JournalClient, auth Authorizer) JournalClient {
	return &authClient{auth: auth, jc: jc}
}

type authClient struct {
	auth Authorizer
	jc   JournalClient
}

func (a *authClient) List(ctx context.Context, in *ListRequest, opts ...grpc.CallOption) (Journal_ListClient, error) {
	var claims, ok = GetClaims(ctx)
	if !ok {
		claims = Claims{
			Capability: Capability_LIST,
			Selector:   in.Selector,
		}
	}
	if ctx, err := a.auth.Authorize(ctx, claims, withExp(in.Watch)); err != nil {
		return nil, err
	} else {
		return a.jc.List(ctx, in, opts...)
	}
}

func (a *authClient) Apply(ctx context.Context, in *ApplyRequest, opts ...grpc.CallOption) (*ApplyResponse, error) {
	var claims, ok = GetClaims(ctx)
	if !ok {
		claims = Claims{Capability: Capability_APPLY}
	}
	if ctx, err := a.auth.Authorize(ctx, claims, withExp(false)); err != nil {
		return nil, err
	} else {
		return a.jc.Apply(ctx, in, opts...)
	}
}

func (a *authClient) Read(ctx context.Context, in *ReadRequest, opts ...grpc.CallOption) (Journal_ReadClient, error) {
	var claims, ok = GetClaims(ctx)
	if !ok {
		claims = Claims{
			Capability: Capability_READ,
			Selector: LabelSelector{
				Include: MustLabelSet("name", in.Journal.StripMeta().String()),
			},
		}
	}
	if ctx, err := a.auth.Authorize(ctx, claims, withExp(true)); err != nil {
		return nil, err
	} else {
		return a.jc.Read(ctx, in, opts...)
	}
}

func (a *authClient) Append(ctx context.Context, opts ...grpc.CallOption) (Journal_AppendClient, error) {
	var claims, ok = GetClaims(ctx)
	if !ok {
		panic("Append requires a context having WithClaims")
	}
	if ctx, err := a.auth.Authorize(ctx, claims, withExp(true)); err != nil {
		return nil, err
	} else {
		return a.jc.Append(ctx, opts...)
	}
}

func (a *authClient) Replicate(ctx context.Context, opts ...grpc.CallOption) (Journal_ReplicateClient, error) {
	var claims, ok = GetClaims(ctx)
	if !ok {
		panic("Replicate requires a context having WithClaims")
	}
	if ctx, err := a.auth.Authorize(ctx, claims, withExp(true)); err != nil {
		return nil, err
	} else {
		return a.jc.Replicate(ctx, opts...)
	}
}

func (a *authClient) ListFragments(ctx context.Context, in *FragmentsRequest, opts ...grpc.CallOption) (*FragmentsResponse, error) {
	var claims, ok = GetClaims(ctx)
	if !ok {
		claims = Claims{
			Capability: Capability_READ, // Allows direct fragment access.
			Selector: LabelSelector{
				Include: MustLabelSet("name", in.Journal.StripMeta().String()),
			},
		}
	}
	if ctx, err := a.auth.Authorize(ctx, claims, withExp(false)); err != nil {
		return nil, err
	} else {
		return a.jc.ListFragments(ctx, in, opts...)
	}
}

func withExp(blocking bool) time.Duration {
	if blocking {
		return time.Hour
	} else {
		return time.Minute
	}
}

// AuthJournalServer is similar to JournalServer except:
//   - Requests have already been verified with accompanying Claims.
//   - The Context or Stream.Context() argument may be subject to a deadline
//     bound to the expiration of the user's Claims.
type AuthJournalServer interface {
	List(Claims, *ListRequest, Journal_ListServer) error
	Apply(context.Context, Claims, *ApplyRequest) (*ApplyResponse, error)
	Read(Claims, *ReadRequest, Journal_ReadServer) error
	Append(Claims, Journal_AppendServer) error
	Replicate(Claims, Journal_ReplicateServer) error
	ListFragments(context.Context, Claims, *FragmentsRequest) (*FragmentsResponse, error)
}

// NewAuthJournalServer adapts an AuthJournalServer into a JournalServer by
// using the provided Verifier to verify incoming request Authorizations.
func NewAuthJournalServer(ajs AuthJournalServer, verifier Verifier) JournalServer {
	return &authServer{
		inner:    ajs,
		verifier: verifier,
	}
}

type authServer struct {
	inner    AuthJournalServer
	verifier Verifier
}

func (s *authServer) List(req *ListRequest, stream Journal_ListServer) error {
	if ctx, cancel, claims, err := s.verifier.Verify(stream.Context(), Capability_LIST); err != nil {
		return err
	} else {
		defer cancel()
		return s.inner.List(claims, req, authListServer{ctx, stream})
	}
}

func (s *authServer) Apply(ctx context.Context, req *ApplyRequest) (*ApplyResponse, error) {
	if ctx, cancel, claims, err := s.verifier.Verify(ctx, Capability_APPLY); err != nil {
		return nil, err
	} else {
		defer cancel()
		return s.inner.Apply(ctx, claims, req)
	}
}

func (s *authServer) Read(req *ReadRequest, stream Journal_ReadServer) error {
	if ctx, cancel, claims, err := s.verifier.Verify(stream.Context(), Capability_READ); err != nil {
		return err
	} else {
		defer cancel()
		return s.inner.Read(claims, req, authReadServer{ctx, stream})
	}
}

func (s *authServer) Append(stream Journal_AppendServer) error {
	if ctx, cancel, claims, err := s.verifier.Verify(stream.Context(), Capability_APPEND); err != nil {
		return err
	} else {
		defer cancel()
		return s.inner.Append(claims, authAppendServer{ctx, stream})
	}
}

func (s *authServer) Replicate(stream Journal_ReplicateServer) error {
	if ctx, cancel, claims, err := s.verifier.Verify(stream.Context(), Capability_REPLICATE); err != nil {
		return err
	} else {
		defer cancel()
		return s.inner.Replicate(claims, authReplicateServer{ctx, stream})
	}
}

func (s *authServer) ListFragments(ctx context.Context, req *FragmentsRequest) (*FragmentsResponse, error) {
	if ctx, cancel, claims, err := s.verifier.Verify(ctx, Capability_READ); err != nil {
		return nil, err
	} else {
		defer cancel()
		return s.inner.ListFragments(ctx, claims, req)
	}
}

var _ JournalServer = &authServer{}
var _ JournalClient = &authClient{}

type claimsCtxKey struct{}

type authListServer struct {
	ctx context.Context
	Journal_ListServer
}
type authReadServer struct {
	ctx context.Context
	Journal_ReadServer
}
type authAppendServer struct {
	ctx context.Context
	Journal_AppendServer
}
type authReplicateServer struct {
	ctx context.Context
	Journal_ReplicateServer
}

func (s authListServer) Context() context.Context      { return s.ctx }
func (s authReadServer) Context() context.Context      { return s.ctx }
func (s authAppendServer) Context() context.Context    { return s.ctx }
func (s authReplicateServer) Context() context.Context { return s.ctx }
