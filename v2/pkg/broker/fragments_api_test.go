package broker

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
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

	var fixture = getFragmentTuplesFixture()
	peer.FragmentsFunc = func(ctx context.Context, req *pb.FragmentsRequest) (*pb.FragmentsResponse, error) {
		c.Check(req, gc.DeepEquals, &pb.FragmentsRequest{
			Header:     &res.Header,
			Journal:    "a/journal",
			Begin:      time.Time{},
			End:        time.Time{},
			PageToken:  0,
			DoNotProxy: false,
		})
		return &pb.FragmentsResponse{
			Status:    pb.Status_OK,
			Header:    &res.Header,
			Fragments: fixture,
			PageToken: fixture[5].Fragment.End,
		}, nil
	}

	var ctx = pb.WithDispatchDefault(tf.ctx)
	var request = &pb.FragmentsRequest{
		Journal:    "a/journal",
		Begin:      time.Time{},
		End:        time.Time{},
		PageToken:  0,
		DoNotProxy: false,
	}

	var resp, err = broker.MustClient().Fragments(ctx, request)
	c.Check(err, gc.IsNil)
	expectFragmentsResponse(c, resp, &pb.FragmentsResponse{
		Status:    pb.Status_OK,
		Header:    &res.Header,
		Fragments: fixture,
		PageToken: fixture[5].Fragment.End,
	})
}

func (s *FragmentsSuite) TestGetFragmentTuplesBoundedRange(c *gc.C) {
	var req = pb.FragmentsRequest{
		Journal:      "a/journal",
		Begin:        time.Unix(100, 0),
		End:          time.Unix(180, 0),
		PageToken:    0,
		PageLimit:    int32(defaultPageLimit),
		SignatureTTL: &defaultSignatureTTL,
	}

	var fixture = getFragmentTuplesFixture()
	var fragmentSet = buildFragmentSet(fixture)
	var expected = []*pb.FragmentsResponse_FragmentTuple{
		fixture[1],
		fixture[3],
	}
	var tuples, err = getFragmentTuples(&req, fragmentSet)
	c.Check(err, gc.IsNil)
	c.Check(tuples, gc.DeepEquals, expected)
}

func (s *FragmentsSuite) TestGetFragmentTuplesUnboundedRange(c *gc.C) {
	var req = pb.FragmentsRequest{
		Journal:      "a/journal",
		Begin:        time.Time{},
		End:          time.Time{},
		PageToken:    0,
		PageLimit:    3,
		SignatureTTL: &defaultSignatureTTL,
	}

	var fixture = getFragmentTuplesFixture()
	var fragmentSet = buildFragmentSet(fixture)
	// Return when page is full.
	var tuples, err = getFragmentTuples(&req, fragmentSet)
	c.Check(err, gc.IsNil)
	c.Check(tuples, gc.DeepEquals, fixture[:3])

	req = pb.FragmentsRequest{
		Journal:      "a/journal",
		Begin:        time.Time{},
		End:          time.Time{},
		PageToken:    tuples[len(tuples)-1].Fragment.End + 1,
		PageLimit:    3,
		SignatureTTL: &defaultSignatureTTL,
	}

	// Return remainder of fragments in next page.
	tuples, err = getFragmentTuples(&req, fragmentSet)
	c.Check(err, gc.IsNil)
	c.Check(tuples, gc.DeepEquals, fixture[3:])
}

func (s *FragmentsSuite) TestServeFragments(c *gc.C) {
	var tmpdir, err = ioutil.TempDir("", "FragmentSuite.TestServeFragments")
	c.Assert(err, gc.IsNil)

	defer func() { os.RemoveAll(tmpdir) }()
	defer func(s string) { fragment.FileSystemStoreRoot = s }(fragment.FileSystemStoreRoot)
	fragment.FileSystemStoreRoot = tmpdir

	var fixture = getFragmentTuplesFixture()
	var paths = []string{
		"root/one/a/journal/0000000000000000-0000000000000028-0000000000000000000000000000000000000000.raw",
		"root/one/a/journal/0000000000000028-000000000000006e-0000000000000000000000000000000000000000.raw",
		"root/one/a/journal/0000000000000063-0000000000000082-0000000000000000000000000000000000000000.raw",
		"root/one/a/journal/0000000000000083-000000000000013e-0000000000000000000000000000000000000000.raw",
		"root/one/a/journal/000000000000013f-0000000000000190-0000000000000000000000000000000000000000.raw",
	}

	for i, path := range paths {
		path = filepath.Join(tmpdir, filepath.FromSlash(path))
		c.Assert(os.MkdirAll(filepath.Dir(path), 0700), gc.IsNil)
		c.Assert(ioutil.WriteFile(path, []byte("data"), 0600), gc.IsNil)
		c.Assert(os.Chtimes(path, time.Now(), fixture[i].Fragment.ModTime), gc.IsNil)
	}

	var ctx = context.Background()
	var res = resolution{
		journalSpec: &pb.JournalSpec{
			Fragment: pb.JournalSpec_Fragment{Stores: []pb.FragmentStore{pb.FragmentStore("file:///root/one/")}},
		},
	}
	var resp *pb.FragmentsResponse

	// Fragments should be returned within the specified range.
	resp, err = serveFragments(ctx, &pb.FragmentsRequest{
		Journal:      "a/journal",
		Begin:        time.Unix(100, 0),
		End:          time.Unix(180, 0),
		PageToken:    0,
		SignatureTTL: &defaultSignatureTTL,
	}, res)
	c.Check(err, gc.IsNil)
	expectFragmentsResponse(c, resp, &pb.FragmentsResponse{
		Status: pb.Status_OK,
		Header: &res.Header,
		Fragments: []*pb.FragmentsResponse_FragmentTuple{
			fixture[1],
			fixture[3],
		},
		PageToken: fixture[3].Fragment.End + 1,
	})

	// No fragments should be returned, and PageToken should not be advanced after the end has been reached
	resp, err = serveFragments(ctx, &pb.FragmentsRequest{
		Journal:      "a/journal",
		Begin:        time.Unix(100, 0),
		End:          time.Unix(180, 0),
		PageToken:    fixture[5].Fragment.End + 1,
		SignatureTTL: &defaultSignatureTTL,
	}, res)
	c.Check(err, gc.IsNil)
	expectFragmentsResponse(c, resp, &pb.FragmentsResponse{
		Status:    pb.Status_OK,
		Header:    &res.Header,
		Fragments: []*pb.FragmentsResponse_FragmentTuple{},
		PageToken: fixture[5].Fragment.End + 1,
	})
}

// return a fixture which can be modified as needed over the course of a test.
var getFragmentTuplesFixture = func() []*pb.FragmentsResponse_FragmentTuple {
	return []*pb.FragmentsResponse_FragmentTuple{
		{
			Fragment: pb.Fragment{
				Journal:          "a/journal",
				Begin:            0,
				End:              40,
				ModTime:          time.Time{},
				BackingStore:     pb.FragmentStore("file:///root/one/"),
				CompressionCodec: pb.CompressionCodec_NONE,
			},
			SignedURL: "file:///root/one/a/journal/0000000000000000-0000000000000028-0000000000000000000000000000000000000000.raw",
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
			SignedURL: "file:///root/one/a/journal/0000000000000028-000000000000006e-0000000000000000000000000000000000000000.raw",
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
			SignedURL: "file:///root/one/a/journal/0000000000000063-0000000000000082-0000000000000000000000000000000000000000.raw",
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
			SignedURL: "file:///root/one/a/journal/0000000000000083-000000000000013e-0000000000000000000000000000000000000000.raw",
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
			SignedURL: "file:///root/one/a/journal/000000000000013f-0000000000000190-0000000000000000000000000000000000000000.raw",
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

func buildFragmentSet(fragments []*pb.FragmentsResponse_FragmentTuple) fragment.CoverSet {
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
