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

	var singedFragments []*pb.FragmentsResponse_SignedFragment
	var nextPageToken int64
	singedFragments, nextPageToken, err = buildSignedFragments(req, res.replica.index)
	if err != nil {
		return resp, err
	}
	resp.Fragments = singedFragments
	resp.NextPageToken = nextPageToken

	return resp, nil
}

// Returns a list of FragmentsResponse_SignedFragment corresponding to a query as well as the NextPageToken to be used for subsequent
// requests. If NextPageToken is nil there are no more fragments left in this page.
func buildSignedFragments(req *pb.FragmentsRequest, index *fragment.Index) ([]*pb.FragmentsResponse_SignedFragment, int64, error) {
	var singedFragments = make([]*pb.FragmentsResponse_SignedFragment, 0, req.PageLimit)
	var nextPageToken int64
	var iterator = index.CreateIterator(req.NextPageToken)
	defer iterator.Close()

	var f fragment.Fragment
	var err error
	for {
		f, err = iterator.Next()
		if err != nil {
			break
		}
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

	// If the singedFragments is not empty and there are more fragments to be
	// returned set the nextPageToken to the next fragment.
	if len(singedFragments) > 0 && err != fragment.ErrDoneIterator {
		nextPageToken = f.Begin
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
