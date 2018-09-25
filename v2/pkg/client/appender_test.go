package client

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/LiveRamp/gazette/pkg/broker/teststub"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
	gc "github.com/go-check/check"
)

type AppenderSuite struct{}

func (s *AppenderSuite) TestCommitSuccess(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var routes = make(map[pb.Journal]*pb.Route)
	var client = routeWrapper{broker.MustClient(), routes}

	var a = NewAppender(ctx, client, pb.AppendRequest{Journal: "a/journal"})

	go func() {
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Journal: "a/journal"})
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Content: []byte("foo")})
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Content: []byte("bar")})
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{})
		c.Check(<-broker.AppendReqCh, gc.IsNil) // Client EOF.

		broker.AppendRespCh <- &pb.AppendResponse{
			Status: pb.Status_OK,
			Header: headerFixture,
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
	c.Check(routes["a/journal"], gc.DeepEquals, &pb.Route{
		Members: []pb.ProcessSpec_ID{{Zone: "a", Suffix: "broker"}},
		Primary: 0,
	})
}

func (s *AppenderSuite) TestBrokerWriteError(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var routes = make(map[pb.Journal]*pb.Route)
	var client = routeWrapper{broker.MustClient(), routes}

	var a = NewAppender(ctx, client, pb.AppendRequest{Journal: "a/journal"})

	go func() {
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Journal: "a/journal"})
		broker.ErrCh <- errors.New("an error")
	}()

	var n, err = a.Write([]byte("foo"))
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, 3)

	for err == nil {
		time.Sleep(time.Millisecond)
		n, err = a.Write([]byte("x"))
	}
	// NOTE(johnny): For some reason, gRPC translates the broker error into an EOF on attempt Send.
	c.Check(err, gc.Equals, io.EOF)
	c.Check(n, gc.Equals, 0)

	// Expect Appender cleared any Route advisement.
	c.Check(routes["a/journal"], gc.IsNil)
	c.Check(routes, gc.HasLen, 1)
}

func (s *AppenderSuite) TestBrokerCommitError(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)

	var cases = []struct {
		finish func()
		err    string
	}{
		// Case: return a straight-up error.
		{
			finish: func() { broker.ErrCh <- errors.New("an error") },
			err:    `rpc error: code = Unknown desc = an error`,
		},
		// Case: return a response which fails validation.
		{
			finish: func() {
				broker.AppendRespCh <- &pb.AppendResponse{
					Status: pb.Status_OK,
					Header: headerFixture,
					Commit: &pb.Fragment{Begin: 1, End: 0},
				}
			},
			err: `Commit.Journal: invalid length .*`,
		},
		// Case: return a response with non-OK status.
		{
			finish: func() {
				broker.AppendRespCh <- &pb.AppendResponse{
					Status: pb.Status_NOT_JOURNAL_PRIMARY_BROKER,
					Header: headerFixture,
					Commit: &pb.Fragment{
						Journal:          "a/journal",
						CompressionCodec: pb.CompressionCodec_NONE,
					},
				}
			},
			err: `NOT_JOURNAL_PRIMARY_BROKER`,
		},
	}

	for _, tc := range cases {

		var routes = make(map[pb.Journal]*pb.Route)
		var client = routeWrapper{broker.MustClient(), routes}
		var a = NewAppender(ctx, client, pb.AppendRequest{Journal: "a/journal"})

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

		c.Check(a.Close(), gc.ErrorMatches, tc.err)

		// Expect Appender cleared any Route advisement.
		c.Check(routes["a/journal"], gc.IsNil)
		c.Check(routes, gc.HasLen, 1)
	}
}

var _ = gc.Suite(&AppenderSuite{})
