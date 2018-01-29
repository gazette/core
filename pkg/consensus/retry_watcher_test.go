package consensus

import (
	"context"
	"errors"
	"time"

	gc "github.com/go-check/check"

	etcd "github.com/coreos/etcd/client"
)

type RetryWatcherSuite struct{}

func (s *RetryWatcherSuite) TestFoo(c *gc.C) {
	var ctx = context.Background()
	var keys MockKeysAPI
	var watcher MockWatcher
	var refreshTicker = make(chan time.Time)

	var rwatcher = RetryWatcher(&keys, "/key",
		&etcd.GetOptions{Recursive: true},
		&etcd.WatcherOptions{Recursive: true}, refreshTicker)

	// Expect first Next is mapped to a Get, and builds a Watcher.
	keys.On("Get", ctx, "/key", &etcd.GetOptions{Recursive: true}).
		Return(&etcd.Response{
			Action: "get",
			Index:  1234,
			Node:   &etcd.Node{Key: "/key", Value: "one"},
		}, nil).Once()

	keys.On("Watcher", "/key",
		&etcd.WatcherOptions{Recursive: true, AfterIndex: 1234}).Return(&watcher).Once()

	resp, err := rwatcher.Next(ctx)
	c.Check(resp.Node, gc.DeepEquals, &etcd.Node{Key: "/key", Value: "one"})
	c.Check(err, gc.IsNil)

	// Next is then passed through.
	watcher.On("Next", ctx).Return(&etcd.Response{
		Action: "set", Node: &etcd.Node{Key: "/key", Value: "two"}}, nil).Once()

	resp, err = rwatcher.Next(ctx)
	c.Check(resp.Node, gc.DeepEquals, &etcd.Node{Key: "/key", Value: "two"})
	c.Check(err, gc.IsNil)

	// Non-etcd errors do not restart the Watcher.
	watcher.On("Next", ctx).Return(nil, errors.New("foobar")).Once()

	resp, err = rwatcher.Next(ctx)
	c.Check(resp, gc.IsNil)
	c.Check(err, gc.ErrorMatches, "foobar")

	// Etcd errors do clear the current Watcher. They cause a full refresh,
	// and start a new Watcher.
	watcher.On("Next", ctx).Return(nil,
		etcd.Error{Code: etcd.ErrorCodeEventIndexCleared}).Once()

	resp, err = rwatcher.Next(ctx)
	c.Check(resp, gc.IsNil)
	c.Check(err, gc.ErrorMatches, "401.*")

	// After refresh period, a full refresh is forced
	// and a new Watcher is started.
	go func() {
		refreshTicker <- time.Now()
		close(refreshTicker)
	}()
	keys.On("Get", ctx, "/key", &etcd.GetOptions{Recursive: true}).
		Return(&etcd.Response{
			Action: "get",
			Index:  3456,
			Node:   &etcd.Node{Key: "/key", Value: "four"},
		}, nil).Once()
	keys.On("Watcher", "/key",
		&etcd.WatcherOptions{Recursive: true, AfterIndex: 3456}).Return(&watcher).Once()
	resp, err = rwatcher.Next(ctx)
	c.Check(resp.Node, gc.DeepEquals, &etcd.Node{Key: "/key", Value: "four"})
	c.Check(err, gc.IsNil)

	watcher.AssertExpectations(c)
	keys.AssertExpectations(c)
}

var _ = gc.Suite(&RetryWatcherSuite{})
