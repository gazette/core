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

func (s *FragmentsSuite) TestGetFragmentTuplesBoundedRange(c *gc.C) {
	var req = pb.FragmentsRequest{
		Begin:        time.Unix(100, 0),
		End:          time.Unix(180, 0),
		PageToken:    0,
		PageLimit:    int32(defaultPageLimit),
		SignatureTTL: &defaultSignatureTTL,
	}

	var fragmentSet = buildFragmentSet(fragmentFixtures)
	var expected = []*pb.FragmentsResponse_FragmentTuple{
		{
			Fragment:  &fragmentFixtures[1],
			SignedURL: filePathFixture[1],
		},
		{
			Fragment:  &fragmentFixtures[3],
			SignedURL: filePathFixture[3],
		},
	}
	var tuples, err = getFragmentTuples(&req, fragmentSet)
	c.Check(err, gc.IsNil)
	c.Check(tuples, gc.DeepEquals, expected)
}

func (s *FragmentsSuite) TestGetFragmentTuplesUnboundedRange(c *gc.C) {
	var req = pb.FragmentsRequest{
		Begin:        time.Time{},
		End:          time.Time{},
		PageToken:    0,
		PageLimit:    3,
		SignatureTTL: &defaultSignatureTTL,
	}

	var fragmentSet = buildFragmentSet(fragmentFixtures)
	var expected = []*pb.FragmentsResponse_FragmentTuple{
		{
			Fragment:  &fragmentFixtures[0],
			SignedURL: filePathFixture[0],
		},
		{
			Fragment:  &fragmentFixtures[1],
			SignedURL: filePathFixture[1],
		},
		{
			Fragment:  &fragmentFixtures[2],
			SignedURL: filePathFixture[2],
		},
		{
			Fragment:  &fragmentFixtures[3],
			SignedURL: filePathFixture[3],
		},
		{
			Fragment:  &fragmentFixtures[4],
			SignedURL: filePathFixture[4],
		},
		{
			Fragment: &fragmentFixtures[5],
		},
	}
	// Return when page is full.
	var tuples, err = getFragmentTuples(&req, fragmentSet)
	c.Check(err, gc.IsNil)
	c.Check(tuples, gc.DeepEquals, expected[:3])

	req = pb.FragmentsRequest{
		Begin:        time.Time{},
		End:          time.Time{},
		PageToken:    tuples[len(tuples)-1].Fragment.End + 1,
		PageLimit:    3,
		SignatureTTL: &defaultSignatureTTL,
	}

	// Return remainder of fragments in next page.
	tuples, err = getFragmentTuples(&req, fragmentSet)
	c.Check(err, gc.IsNil)
	c.Check(tuples, gc.DeepEquals, expected[3:])
}

func (s *FragmentsSuite) TestServeFragments(c *gc.C) {
	var tmpdir, err = ioutil.TempDir("", "FragmentSuite.TestServeFragments")
	c.Assert(err, gc.IsNil)

	defer func() { os.RemoveAll(tmpdir) }()
	defer func(s string) { fragment.FileSystemStoreRoot = s }(fragment.FileSystemStoreRoot)
	fragment.FileSystemStoreRoot = tmpdir

	var paths = []string{
		"root/one/validJournal/0000000000000000-0000000000000028-0000000000000000000000000000000000000000.raw",
		"root/one/validJournal/0000000000000028-000000000000006e-0000000000000000000000000000000000000000.raw",
		"root/one/validJournal/0000000000000063-0000000000000082-0000000000000000000000000000000000000000.raw",
		"root/one/validJournal/0000000000000083-000000000000013e-0000000000000000000000000000000000000000.raw",
		"root/one/validJournal/000000000000013f-0000000000000190-0000000000000000000000000000000000000000.raw",
	}

	for i, path := range paths {
		path = filepath.Join(tmpdir, filepath.FromSlash(path))
		c.Assert(os.MkdirAll(filepath.Dir(path), 0700), gc.IsNil)
		c.Assert(ioutil.WriteFile(path, []byte("data"), 0600), gc.IsNil)
		c.Assert(os.Chtimes(path, time.Now(), fragmentFixtures[i].ModTime), gc.IsNil)
	}

	var ctx = context.Background()
	var spec = &pb.JournalSpec{
		Fragment: pb.JournalSpec_Fragment{Stores: []pb.FragmentStore{pb.FragmentStore("file:///root/one/")}},
	}
	var resp *pb.FragmentsResponse

	// Fragments should be returned within the specified range.
	var expected = &pb.FragmentsResponse{
		Status: pb.Status_OK,
		Fragments: []*pb.FragmentsResponse_FragmentTuple{
			{
				Fragment:  &fragmentFixtures[1],
				SignedURL: filePathFixture[1],
			},
			{
				Fragment:  &fragmentFixtures[3],
				SignedURL: filePathFixture[3],
			},
		},
		PageToken: fragmentFixtures[3].End + 1,
	}
	var req = &pb.FragmentsRequest{
		Begin:        time.Unix(100, 0),
		End:          time.Unix(180, 0),
		PageToken:    0,
		SignatureTTL: &defaultSignatureTTL,
	}
	resp, err = serveFragments(ctx, req, spec)
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, expected)

	// No fragments should be returned, and PageToken should not be advanced after the end has been reached
	expected = &pb.FragmentsResponse{
		Status:    pb.Status_OK,
		Fragments: []*pb.FragmentsResponse_FragmentTuple{},
		PageToken: fragmentFixtures[5].End + 1,
	}
	req = &pb.FragmentsRequest{
		Begin:        time.Unix(100, 0),
		End:          time.Unix(180, 0),
		PageToken:    fragmentFixtures[5].End + 1,
		SignatureTTL: &defaultSignatureTTL,
	}
	resp, err = serveFragments(ctx, req, spec)
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, expected)
}

var filePathFixture = []string{
	"file:///root/one/validJournal/0000000000000000-0000000000000028-0000000000000000000000000000000000000000.raw",
	"file:///root/one/validJournal/0000000000000028-000000000000006e-0000000000000000000000000000000000000000.raw",
	"file:///root/one/validJournal/0000000000000063-0000000000000082-0000000000000000000000000000000000000000.raw",
	"file:///root/one/validJournal/0000000000000083-000000000000013e-0000000000000000000000000000000000000000.raw",
	"file:///root/one/validJournal/000000000000013f-0000000000000190-0000000000000000000000000000000000000000.raw",
}

var fragmentFixtures = []pb.Fragment{
	{
		Journal:          "validJournal",
		Begin:            0,
		End:              40,
		ModTime:          time.Time{},
		BackingStore:     pb.FragmentStore("file:///root/one/"),
		CompressionCodec: pb.CompressionCodec_NONE,
	},
	{
		Journal:          "validJournal",
		Begin:            40,
		End:              110,
		ModTime:          time.Unix(101, 0),
		BackingStore:     pb.FragmentStore("file:///root/one/"),
		CompressionCodec: pb.CompressionCodec_NONE,
	},
	{
		Journal:          "validJournal",
		Begin:            99,
		End:              130,
		ModTime:          time.Unix(200, 0),
		BackingStore:     pb.FragmentStore("file:///root/one/"),
		CompressionCodec: pb.CompressionCodec_NONE,
	},
	{
		Journal:          "validJournal",
		Begin:            131,
		End:              318,
		ModTime:          time.Unix(150, 0),
		BackingStore:     pb.FragmentStore("file:///root/one/"),
		CompressionCodec: pb.CompressionCodec_NONE,
	},
	{
		Journal:          "validJournal",
		Begin:            319,
		End:              400,
		ModTime:          time.Unix(290, 0),
		BackingStore:     pb.FragmentStore("file:///root/one/"),
		CompressionCodec: pb.CompressionCodec_NONE,
	},
	{
		Journal: "validJournal",
		Begin:   380,
		End:     600,
	},
}

func buildFragmentSet(fragments []pb.Fragment) fragment.CoverSet {
	var set = fragment.CoverSet{}
	for _, f := range fragments {
		set, _ = set.Add(fragment.Fragment{Fragment: f})
	}
	return set
}

var _ = gc.Suite(&FragmentsSuite{})
