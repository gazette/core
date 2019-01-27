package broker

import (
	"context"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/fragment"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
)

var (
	defaultSignatureTTL = time.Hour * 24
	defaultPageLimit    = int32(1000)
)

// Fragments dispatches the JournalServer.Fragments API.
func (svc *Service) Fragments(ctx context.Context, req *pb.FragmentsRequest) (*pb.FragmentsResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	var res, err = svc.resolver.resolve(resolveArgs{
		ctx:                   ctx,
		journal:               req.Journal,
		mayProxy:              !req.DoNotProxy,
		requirePrimary:        false,
		requireFullAssignment: false,
		proxyHeader:           req.Header,
	})

	if err != nil {
		return nil, err
	} else if res.status != pb.Status_OK {
		return &pb.FragmentsResponse{Status: res.status, Header: &res.Header}, nil
	} else if !res.journalSpec.Flags.MayRead() {
		return (&pb.FragmentsResponse{Status: pb.Status_NOT_ALLOWED, Header: &res.Header}), nil
	} else if res.replica == nil {
		req.Header = &res.Header // Attach resolved Header to |req|, which we'll forward.
		ctx = pb.WithDispatchRoute(ctx, req.Header.Route, req.Header.ProcessId)

		return svc.jc.Fragments(ctx, req)
	}

	return serveFragments(ctx, req, res)
}

func serveFragments(ctx context.Context, req *pb.FragmentsRequest, res resolution) (*pb.FragmentsResponse, error) {
	if req.SignatureTTL == nil {
		req.SignatureTTL = &defaultSignatureTTL
	}
	if req.PageLimit == 0 {
		req.PageLimit = int32(defaultPageLimit)
	}

	var err error
	if err = res.replica.index.WaitForFirstRemoteRefresh(ctx); err != nil {
		return nil, pb.ExtendContext(err, "error waiting for index")
	}

	var resp = &pb.FragmentsResponse{
		Status: pb.Status_OK,
		Header: &res.Header,
	}
	if err := res.replica.index.Inspect(func(fragmentSet fragment.CoverSet) error {
		var singedFragments, nextPageToken, err = buildSignedFragments(req, fragmentSet)
		if err != nil {
			return err
		}
		resp.Fragments = singedFragments
		resp.NextPageToken = nextPageToken

		return nil
	}); err != nil {
		return resp, err
	}

	return resp, nil
}

// Returns a list of FragmentsResponse_SignedFragment corresponding to a query as well as the NextPageToken to be used for subsequent
// requests. If NextPageToken is nil there are no more fragments left in this page.
func buildSignedFragments(req *pb.FragmentsRequest, fragmentSet fragment.CoverSet) ([]*pb.FragmentsResponse_SignedFragment, int64, error) {
	var singedFragments = make([]*pb.FragmentsResponse_SignedFragment, 0, req.PageLimit)
	var nextPageToken int64
	var ind, found = fragmentSet.LongestOverlappingFragment(req.NextPageToken)
	// If the NextPageToken offset is larger than the largest fragment all valid fragments
	// have been returned in previous pages. Return empty slice of fragment tuples.
	if !found && ind == len(fragmentSet) {
		return singedFragments, nextPageToken, nil
	}

	var i int
	var f fragment.Fragment
	for i, f = range fragmentSet[ind:] {
		if len(singedFragments) == cap(singedFragments) {
			break
		}

		// If the query is does not specify an EndModTime and there is no BackingStore or ModTime this is a live fragment
		// and can be added to the list of singedFragments, but no signedURL can be constructed for this fragment.
		if req.EndModTime.IsZero() && (f.BackingStore == "" && f.ModTime.IsZero()) {
			singedFragments = append(singedFragments, &pb.FragmentsResponse_SignedFragment{Fragment: f.Fragment})
			continue
		}

		// Ensure the current fragment is within the time bounds of the query.
		if (req.BeginModTime.Equal(f.ModTime) || req.BeginModTime.Before(f.ModTime)) &&
			(req.EndModTime.IsZero() || req.EndModTime.After(f.ModTime)) {
			var singedFragment, err = buildSignedFragment(f.Fragment, *req.SignatureTTL)
			if err != nil {
				return singedFragments, nextPageToken, err
			}
			singedFragments = append(singedFragments, singedFragment)
		}
	}

	var nextIndex = i + ind
	if len(singedFragments) > 0 && nextIndex < len(fragmentSet)-1 {
		nextPageToken = fragmentSet[nextIndex].Begin
	}
	return singedFragments, nextPageToken, nil
}

func buildSignedFragment(f pb.Fragment, ttl time.Duration) (*pb.FragmentsResponse_SignedFragment, error) {
	var signedURL string
	var err error
	if f.BackingStore != "" {
		signedURL, err = fragment.SignGetURL(f, ttl)
		if err != nil {
			return nil, err
		}
	}

	return &pb.FragmentsResponse_SignedFragment{
		Fragment:  f,
		SignedUrl: signedURL,
	}, nil
}
