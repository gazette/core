package protocol

import (
	context "context"
	time "time"

	pb "go.gazette.dev/core/broker/protocol"
	grpc "google.golang.org/grpc"
)

// NewAuthShardClient returns a ShardClient which uses the Authorizer
// to obtain and attach an Authorization bearer token to every issued request.
func NewAuthShardClient(sc ShardClient, auth pb.Authorizer) ShardClient {
	return &authShardClient{auth: auth, sc: sc}
}

type authShardClient struct {
	auth pb.Authorizer
	sc   ShardClient
}

func (a *authShardClient) Stat(ctx context.Context, in *StatRequest, opts ...grpc.CallOption) (*StatResponse, error) {
	var claims, ok = pb.GetClaims(ctx)
	if !ok {
		claims = pb.Claims{
			Capability: pb.Capability_READ,
			Selector: pb.LabelSelector{
				Include: pb.MustLabelSet("id", in.Shard.String()),
			},
		}
	}
	if ctx, err := a.auth.Authorize(ctx, claims, withExp(false)); err != nil {
		return nil, err
	} else {
		return a.sc.Stat(ctx, in, opts...)
	}
}

func (a *authShardClient) List(ctx context.Context, in *ListRequest, opts ...grpc.CallOption) (*ListResponse, error) {
	var claims, ok = pb.GetClaims(ctx)
	if !ok {
		claims = pb.Claims{
			Capability: pb.Capability_LIST,
			Selector:   in.Selector,
		}
	}
	if ctx, err := a.auth.Authorize(ctx, claims, withExp(false)); err != nil {
		return nil, err
	} else {
		return a.sc.List(ctx, in, opts...)
	}
}

func (a *authShardClient) Apply(ctx context.Context, in *ApplyRequest, opts ...grpc.CallOption) (*ApplyResponse, error) {
	var claims, ok = pb.GetClaims(ctx)
	if !ok {
		claims = pb.Claims{Capability: pb.Capability_APPLY}
	}
	if ctx, err := a.auth.Authorize(ctx, claims, withExp(false)); err != nil {
		return nil, err
	} else {
		return a.sc.Apply(ctx, in, opts...)
	}
}

func (a *authShardClient) GetHints(ctx context.Context, in *GetHintsRequest, opts ...grpc.CallOption) (*GetHintsResponse, error) {
	var claims, ok = pb.GetClaims(ctx)
	if !ok {
		claims = pb.Claims{
			Capability: pb.Capability_READ,
			Selector: pb.LabelSelector{
				Include: pb.MustLabelSet("id", in.Shard.String()),
			},
		}
	}
	if ctx, err := a.auth.Authorize(ctx, claims, withExp(false)); err != nil {
		return nil, err
	} else {
		return a.sc.GetHints(ctx, in, opts...)
	}
}

func (a *authShardClient) Unassign(ctx context.Context, in *UnassignRequest, opts ...grpc.CallOption) (*UnassignResponse, error) {
	var claims, ok = pb.GetClaims(ctx)
	if !ok {
		claims = pb.Claims{Capability: pb.Capability_APPLY}
		for _, id := range in.Shards {
			claims.Selector.Include.AddValue("id", id.String())
		}
	}
	if ctx, err := a.auth.Authorize(ctx, claims, withExp(false)); err != nil {
		return nil, err
	} else {
		return a.sc.Unassign(ctx, in, opts...)
	}
}

func withExp(blocking bool) time.Duration {
	if blocking {
		return time.Hour
	} else {
		return time.Minute
	}
}

// AuthShardServer is similar to ShardServer except:
//   - Requests have already been verified with accompanying Claims.
//   - The Context or Stream.Context() argument may be subject to a deadline
//     bound to the expiration of the user's Claims.
type AuthShardServer interface {
	Stat(context.Context, pb.Claims, *StatRequest) (*StatResponse, error)
	List(context.Context, pb.Claims, *ListRequest) (*ListResponse, error)
	Apply(context.Context, pb.Claims, *ApplyRequest) (*ApplyResponse, error)
	GetHints(context.Context, pb.Claims, *GetHintsRequest) (*GetHintsResponse, error)
	Unassign(context.Context, pb.Claims, *UnassignRequest) (*UnassignResponse, error)
}

// NewAuthShardServer adapts an AuthShardServer into a ShardServer by
// using the provided Verifier to verify incoming request Authorizations.
func NewAuthShardServer(ajs AuthShardServer, verifier pb.Verifier) ShardServer {
	return &authServer{
		inner:    ajs,
		verifier: verifier,
	}
}

type authServer struct {
	inner    AuthShardServer
	verifier pb.Verifier
}

func (s *authServer) Stat(ctx context.Context, req *StatRequest) (*StatResponse, error) {
	if ctx, cancel, claims, err := s.verifier.Verify(ctx, pb.Capability_READ); err != nil {
		return nil, err
	} else {
		defer cancel()
		return s.inner.Stat(ctx, claims, req)
	}
}

func (s *authServer) List(ctx context.Context, req *ListRequest) (*ListResponse, error) {
	if ctx, cancel, claims, err := s.verifier.Verify(ctx, pb.Capability_LIST); err != nil {
		return nil, err
	} else {
		defer cancel()
		return s.inner.List(ctx, claims, req)
	}
}

func (s *authServer) Apply(ctx context.Context, req *ApplyRequest) (*ApplyResponse, error) {
	if ctx, cancel, claims, err := s.verifier.Verify(ctx, pb.Capability_APPLY); err != nil {
		return nil, err
	} else {
		defer cancel()
		return s.inner.Apply(ctx, claims, req)
	}
}

func (s *authServer) GetHints(ctx context.Context, req *GetHintsRequest) (*GetHintsResponse, error) {
	if ctx, cancel, claims, err := s.verifier.Verify(ctx, pb.Capability_READ); err != nil {
		return nil, err
	} else {
		defer cancel()
		return s.inner.GetHints(ctx, claims, req)
	}
}

func (s *authServer) Unassign(ctx context.Context, req *UnassignRequest) (*UnassignResponse, error) {
	if ctx, cancel, claims, err := s.verifier.Verify(ctx, pb.Capability_APPLY); err != nil {
		return nil, err
	} else {
		defer cancel()
		return s.inner.Unassign(ctx, claims, req)
	}
}

var _ ShardServer = &authServer{}
var _ ShardClient = &authShardClient{}
