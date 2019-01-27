package broker

import (
	"context"
	"sync"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/fragment"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

type FragmentsSuite struct{}

func (s *FragmentsSuite) TestFragmentsProxyCases(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()
	var broker = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReadyReplica)
	var peer = newMockBroker(c, tf, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	newTestJournal(c, tf, pb.JournalSpec{Name: "a/journal", Replication: 1}, peer.id)
	var res, _ = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal", mayProxy: true})

	var fixture = buildSignedFragmentsFixture()
	peer.FragmentsFunc = func(ctx context.Context, req *pb.FragmentsRequest) (*pb.FragmentsResponse, error) {
		c.Check(req, gc.DeepEquals, &pb.FragmentsRequest{
			Header:        &res.Header,
			Journal:       "a/journal",
			BeginModTime:  time.Time{},
			EndModTime:    time.Time{},
			NextPageToken: 0,
			DoNotProxy:    false,
		})
		return &pb.FragmentsResponse{
			Status:        pb.Status_OK,
			Header:        &res.Header,
			Fragments:     fixture,
			NextPageToken: fixture[5].Fragment.End,
		}, nil
	}

	var ctx = pb.WithDispatchDefault(tf.ctx)
	var request = &pb.FragmentsRequest{
		Journal:       "a/journal",
		BeginModTime:  time.Time{},
		EndModTime:    time.Time{},
		NextPageToken: 0,
		DoNotProxy:    false,
	}

	var resp, err = broker.MustClient().Fragments(ctx, request)
	c.Check(err, gc.IsNil)
	expectFragmentsResponse(c, resp, &pb.FragmentsResponse{
		Status:        pb.Status_OK,
		Header:        &res.Header,
		Fragments:     fixture,
		NextPageToken: fixture[5].Fragment.End,
	})
}

func (s *FragmentsSuite) TestbuildSignedFragmentsBoundedRange(c *gc.C) {
	var req = pb.FragmentsRequest{
		Journal:       "a/journal",
		BeginModTime:  time.Unix(100, 0),
		EndModTime:    time.Unix(180, 0),
		NextPageToken: 0,
		PageLimit:     int32(defaultPageLimit),
		SignatureTTL:  &defaultSignatureTTL,
	}

	var fixture = buildSignedFragmentsFixture()
	var fragmentSet = buildFragmentSet(fixture)
	var expected = []*pb.FragmentsResponse_SignedFragment{
		fixture[1],
		fixture[3],
	}
	var signedFragments, nextPageToken, err = buildSignedFragments(&req, fragmentSet)
	c.Check(err, gc.IsNil)
	c.Check(nextPageToken, gc.DeepEquals, int64(0))
	c.Check(signedFragments, gc.DeepEquals, expected)
}

func (s *FragmentsSuite) TestbuildSignedFragmentsUnboundedRange(c *gc.C) {
	var req = pb.FragmentsRequest{
		Journal:       "a/journal",
		BeginModTime:  time.Time{},
		EndModTime:    time.Time{},
		NextPageToken: 0,
		PageLimit:     3,
		SignatureTTL:  &defaultSignatureTTL,
	}

	var fixture = buildSignedFragmentsFixture()
	var fragmentSet = buildFragmentSet(fixture)
	// Return when page is full.
	var signedFragments, nextPageToken, err = buildSignedFragments(&req, fragmentSet)
	c.Check(err, gc.IsNil)
	c.Check(nextPageToken, gc.DeepEquals, fixture[3].Begin)
	c.Check(signedFragments, gc.DeepEquals, fixture[:3])

	req = pb.FragmentsRequest{
		Journal:       "a/journal",
		BeginModTime:  time.Time{},
		EndModTime:    time.Time{},
		NextPageToken: signedFragments[len(signedFragments)-1].Fragment.End + 1,
		PageLimit:     3,
		SignatureTTL:  &defaultSignatureTTL,
	}

	// Return remainder of fragments in next page.
	signedFragments, nextPageToken, err = buildSignedFragments(&req, fragmentSet)
	c.Check(err, gc.IsNil)
	c.Check(nextPageToken, gc.DeepEquals, int64(0))
	c.Check(signedFragments, gc.DeepEquals, fixture[3:])
}

func (s *FragmentsSuite) TestServeFragments(c *gc.C) {
	var fixture = buildSignedFragmentsFixture()
	var res = resolution{
		journalSpec: &pb.JournalSpec{
			Fragment: pb.JournalSpec_Fragment{Stores: []pb.FragmentStore{pb.FragmentStore("file:///root/one/")}},
		},
		replica: newReplica("a/journal"),
	}
	var wg = sync.WaitGroup{}

	wg.Add(1)
	go func() {
		var resp *pb.FragmentsResponse
		var ctx = context.Background()
		var err error
		// Fragments should be returned within the specified range.
		resp, err = serveFragments(ctx, &pb.FragmentsRequest{
			Journal:       "a/journal",
			BeginModTime:  time.Unix(100, 0),
			EndModTime:    time.Unix(180, 0),
			NextPageToken: 0,
			SignatureTTL:  &defaultSignatureTTL,
		}, res)
		c.Check(err, gc.IsNil)
		expectFragmentsResponse(c, resp, &pb.FragmentsResponse{
			Status: pb.Status_OK,
			Header: &res.Header,
			Fragments: []*pb.FragmentsResponse_SignedFragment{
				fixture[1],
				fixture[3],
			},
			NextPageToken: 0,
		})
		wg.Done()
	}()

	// Asynchronously seed the fragment index with fixture data.
	go res.replica.index.ReplaceRemote(buildFragmentSet(fixture))
	wg.Wait()
}

// return a fixture which can be modified as needed over the course of a test.
var buildSignedFragmentsFixture = func() []*pb.FragmentsResponse_SignedFragment {
	return []*pb.FragmentsResponse_SignedFragment{
		{
			Fragment: pb.Fragment{
				Journal:          "a/journal",
				Begin:            0,
				End:              40,
				ModTime:          time.Time{},
				BackingStore:     pb.FragmentStore("file:///root/one/"),
				CompressionCodec: pb.CompressionCodec_NONE,
			},
			SignedUrl: "file:///root/one/a/journal/0000000000000000-0000000000000028-0000000000000000000000000000000000000000.raw",
		},
		{
			Fragment: pb.Fragment{
				Journal:          "a/journal",
				Begin:            40,
				End:              110,
				ModTime:          time.Unix(101, 0),
				BackingStore:     pb.FragmentStore("file:///root/one/"),
				CompressionCodec: pb.CompressionCodec_NONE,
			},
			SignedUrl: "file:///root/one/a/journal/0000000000000028-000000000000006e-0000000000000000000000000000000000000000.raw",
		},
		{
			Fragment: pb.Fragment{
				Journal:          "a/journal",
				Begin:            99,
				End:              130,
				ModTime:          time.Unix(200, 0),
				BackingStore:     pb.FragmentStore("file:///root/one/"),
				CompressionCodec: pb.CompressionCodec_NONE,
			},
			SignedUrl: "file:///root/one/a/journal/0000000000000063-0000000000000082-0000000000000000000000000000000000000000.raw",
		},
		{
			Fragment: pb.Fragment{
				Journal:          "a/journal",
				Begin:            131,
				End:              318,
				ModTime:          time.Unix(150, 0),
				BackingStore:     pb.FragmentStore("file:///root/one/"),
				CompressionCodec: pb.CompressionCodec_NONE,
			},
			SignedUrl: "file:///root/one/a/journal/0000000000000083-000000000000013e-0000000000000000000000000000000000000000.raw",
		},
		{
			Fragment: pb.Fragment{
				Journal:          "a/journal",
				Begin:            319,
				End:              400,
				ModTime:          time.Unix(290, 0),
				BackingStore:     pb.FragmentStore("file:///root/one/"),
				CompressionCodec: pb.CompressionCodec_NONE,
			},
			SignedUrl: "file:///root/one/a/journal/000000000000013f-0000000000000190-0000000000000000000000000000000000000000.raw",
		},
		{
			Fragment: pb.Fragment{
				Journal: "a/journal",
				Begin:   380,
				End:     600,
			},
		},
	}
}

func buildFragmentSet(fragments []*pb.FragmentsResponse_SignedFragment) fragment.CoverSet {
	var set = fragment.CoverSet{}
	for _, f := range fragments {
		set, _ = set.Add(fragment.Fragment{Fragment: f.Fragment})
	}
	return set
}

// expectedFragmentsResponse allows for evaluating pb.FragmentsResponses which contain time.Time values
// which can not be comapred using refelct.DeepEqual.
func expectFragmentsResponse(c *gc.C, resp *pb.FragmentsResponse, expected *pb.FragmentsResponse) {
	for i := range resp.Fragments {
		c.Check(expected.Fragments[i].Fragment.ModTime.Equal(resp.Fragments[i].Fragment.ModTime), gc.Equals, true)
		resp.Fragments[i].Fragment.ModTime, expected.Fragments[i].Fragment.ModTime = time.Time{}, time.Time{}
	}
	c.Check(resp, gc.DeepEquals, expected)
}

var _ = gc.Suite(&FragmentsSuite{})
