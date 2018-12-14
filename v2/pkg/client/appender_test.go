package client

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/broker/teststub"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

type AppenderSuite struct{}

func (s *AppenderSuite) TestCommitSuccess(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)

	var rjc = pb.NewRoutedJournalClient(broker.MustClient(), NewRouteCache(1, time.Hour))
	var a = NewAppender(ctx, rjc, pb.AppendRequest{Journal: "a/journal"})

	// Expect to read a number of request frames from the Appender, then respond.
	go func() {
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Journal: "a/journal"})
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Content: []byte("foo")})
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Content: []byte("bar")})
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{})
		c.Check(<-broker.AppendReqCh, gc.IsNil) // Client EOF.

		broker.AppendRespCh <- &pb.AppendResponse{
			Status: pb.Status_OK,
			Header: buildHeaderFixture(broker),
			Commit: &pb.Fragment{
				Journal:          "a/journal",
				Begin:            100,
				End:              106,
				Sum:              pb.SHA1SumOf("foobar"),
				CompressionCodec: pb.CompressionCodec_NONE,
			},
		}
	}()

	var n, err = a.Write([]byte("foo"))
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, 3)

	n, err = a.Write([]byte("bar"))
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, 3)

	c.Check(a.Close(), gc.IsNil)
	c.Check(a.Response.Commit.Journal, gc.Equals, pb.Journal("a/journal"))

	// Expect Appender advised of the updated Route.
	c.Check(rjc.Route(ctx, "a/journal"), gc.DeepEquals, pb.Route{
		Members:   []pb.ProcessSpec_ID{{Zone: "a", Suffix: "broker"}},
		Endpoints: []pb.Endpoint{broker.Endpoint()},
		Primary:   0,
	})
}

func (s *AppenderSuite) TestBrokerWriteError(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var rjc = pb.NewRoutedJournalClient(broker.MustClient(), pb.NoopDispatchRouter{})
	var a = NewAppender(ctx, rjc, pb.AppendRequest{Journal: "a/journal"})

	go func() {
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Journal: "a/journal"})
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Content: []byte("foo")})
		broker.ErrCh <- errors.New("an error")
	}()

	var n, err = a.Write([]byte("foo"))
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, 3)

	for err == nil {
		time.Sleep(time.Millisecond)
		n, err = a.Write([]byte("x"))
	}
	// NOTE(johnny): For some reason, gRPC translates the remote error into an
	// EOF returned by an attempted SendMsg.
	c.Check(err, gc.Equals, io.EOF)
	c.Check(n, gc.Equals, 0)
}

func (s *AppenderSuite) TestBrokerCommitError(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)

	var cases = []struct {
		finish      func()
		errRe       string
		errVal      error
		cachedRoute int
	}{
		// Case: return a straight-up error.
		{
			finish:      func() { broker.ErrCh <- errors.New("an error") },
			errRe:       `rpc error: code = Unknown desc = an error`,
			cachedRoute: 0,
		},
		// Case: return a response which fails validation.
		{
			finish: func() {
				broker.AppendRespCh <- &pb.AppendResponse{
					Status: pb.Status_OK,
					Header: buildHeaderFixture(broker),
					Commit: &pb.Fragment{Begin: 1, End: 0},
				}
			},
			errRe:       `Commit.Journal: invalid length .*`,
			cachedRoute: 0,
		},
		// Case: known error status (not primary broker).
		{
			finish: func() {
				broker.AppendRespCh <- &pb.AppendResponse{
					Status: pb.Status_NOT_JOURNAL_PRIMARY_BROKER,
					Header: buildHeaderFixture(broker),
				}
			},
			errVal:      ErrNotJournalPrimaryBroker,
			cachedRoute: 1,
		},
		// Case: known error status (wrong append offset).
		{
			finish: func() {
				broker.AppendRespCh <- &pb.AppendResponse{
					Status: pb.Status_WRONG_APPEND_OFFSET,
					Header: buildHeaderFixture(broker),
				}
			},
			errVal:      ErrWrongAppendOffset,
			cachedRoute: 1,
		},
		// Case: other error status.
		{
			finish: func() {
				broker.AppendRespCh <- &pb.AppendResponse{
					Status: pb.Status_OFFSET_NOT_YET_AVAILABLE,
					Header: buildHeaderFixture(broker),
				}
			},
			errRe:       "OFFSET_NOT_YET_AVAILABLE",
			cachedRoute: 1,
		},
	}

	for _, tc := range cases {
		var rc = NewRouteCache(1, time.Hour)
		var rjc = pb.NewRoutedJournalClient(broker.MustClient(), rc)
		var a = NewAppender(ctx, rjc, pb.AppendRequest{Journal: "a/journal"})

		go func() {
			c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Journal: "a/journal"})
			c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Content: []byte("foo")})
			c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{})
			c.Check(<-broker.AppendReqCh, gc.IsNil) // Client EOF.

			tc.finish()
		}()

		var n, err = a.Write([]byte("foo"))
		c.Check(err, gc.IsNil)
		c.Check(n, gc.Equals, 3)
		if tc.errVal != nil {
			c.Check(a.Close(), gc.Equals, tc.errVal)
		} else {
			c.Check(a.Close(), gc.ErrorMatches, tc.errRe)
		}

		// Depending on the case, Close may have updated the cached Route.
		c.Check(rc.cache.Len(), gc.Equals, tc.cachedRoute)
	}
}

func (s *AppenderSuite) TestAppendCases(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)

	// Prime the route cache with an invalid route, which fails with a transport error.
	var rc = NewRouteCache(1, time.Hour)
	rc.UpdateRoute("a/journal", &pb.Route{
		Members:   []pb.ProcessSpec_ID{{Zone: "zone", Suffix: "broker"}},
		Endpoints: []pb.Endpoint{"http://0.0.0.0:0"},
	})
	var rjc = pb.NewRoutedJournalClient(broker.MustClient(), rc)

	go func() {
		var cases = []struct {
			status pb.Status
			err    error
		}{
			// Case 1: Append retries on unavailable transport error.
			{status: pb.Status_OK},
			// Case 1: Append retries on routing error.
			{status: pb.Status_NOT_JOURNAL_PRIMARY_BROKER},
			{status: pb.Status_OK},
			// Case 2: Unexpected status is surfaced.
			{status: pb.Status_INSUFFICIENT_JOURNAL_BROKERS},
			// Case 3: As are errors.
			{err: errors.New("an error")},
		}
		for _, tc := range cases {
			c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Journal: "a/journal"})
			c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Content: []byte("con")})
			c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Content: []byte("tent")})
			c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{})
			c.Check(<-broker.AppendReqCh, gc.IsNil) // Client EOF.

			if tc.err != nil {
				broker.ErrCh <- tc.err
			} else {
				broker.AppendRespCh <- &pb.AppendResponse{
					Status: tc.status,
					Header: buildHeaderFixture(broker),
					Commit: &pb.Fragment{
						Journal:          "a/journal",
						Begin:            1234,
						End:              5678,
						CompressionCodec: pb.CompressionCodec_NONE,
					},
				}
			}
		}

		// Case 4: A broker client reader rolls back the txn.
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Journal: "a/journal"})
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Content: []byte("con")})
		c.Check(<-broker.AppendReqCh, gc.IsNil) // Client EOF w/o empty chunk => rollback.
		broker.ErrCh <- io.ErrUnexpectedEOF
	}()

	var con = strings.NewReader("con")
	var tent = strings.NewReader("tent")

	// Case 1: Transport error is retried, and then succeeds.
	var resp, err = Append(ctx, rjc, pb.AppendRequest{Journal: "a/journal"}, con, tent)
	c.Check(err, gc.IsNil)
	c.Check(resp.Commit, gc.NotNil)

	// Case 2: Routing error is retried, and then succeeds.
	resp, err = Append(ctx, rjc, pb.AppendRequest{Journal: "a/journal"}, con, tent)
	c.Check(err, gc.IsNil)
	c.Check(resp.Commit, gc.NotNil)

	// Case 2: Unexpected status is surfaced.
	_, err = Append(ctx, rjc, pb.AppendRequest{Journal: "a/journal"}, con, tent)
	c.Check(err, gc.ErrorMatches, "INSUFFICIENT_JOURNAL_BROKERS")

	// Case 3: As are errors.
	_, err = Append(ctx, rjc, pb.AppendRequest{Journal: "a/journal"}, con, tent)
	c.Check(err, gc.ErrorMatches, "rpc error: code = Unknown desc = an error")

	// Case 4: A broken reader aborts the txn.
	_, err = Append(ctx, rjc, pb.AppendRequest{Journal: "a/journal"}, con, errReaderAt{}, tent)
	c.Check(err, gc.ErrorMatches, "readerAt error")
}

func (s *AppenderSuite) TestContextErrorCases(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var rjc = pb.NewRoutedJournalClient(broker.MustClient(), pb.NoopDispatchRouter{})
	var content = strings.NewReader("content")

	// Case 1: context is cancelled.
	var caseCtx, caseCancel = context.WithCancel(ctx)
	caseCancel()

	var _, err = Append(caseCtx, rjc, pb.AppendRequest{Journal: "a/journal"}, content)
	c.Check(err, gc.Equals, context.Canceled)

	// Case 2: context reaches deadline.
	caseCtx, _ = context.WithTimeout(ctx, time.Microsecond)
	<-caseCtx.Done() // Block until deadline.

	_, err = Append(caseCtx, rjc, pb.AppendRequest{Journal: "a/journal"}, content)
	c.Check(err, gc.Equals, context.DeadlineExceeded)
}

type errReaderAt struct{}

func (errReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, errors.New("readerAt error")
}

var _ = gc.Suite(&AppenderSuite{})
