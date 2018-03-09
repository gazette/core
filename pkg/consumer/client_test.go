package consumer

import (
	"net"
	"sort"

	gc "github.com/go-check/check"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

type ClientSuite struct{}

func (s *ClientSuite) TestClientInitializationAndUpdate(c *gc.C) {
	var s0, s1, s2 = buildMockServer(c), buildMockServer(c), buildMockServer(c)
	defer s0.srv.GracefulStop()
	defer s1.srv.GracefulStop()
	defer s2.srv.GracefulStop()

	s0.mock.On("CurrentConsumerState", mock.Anything, &Empty{}).Return(
		buildConsumerStateFixture(s0.addr, s1.addr, s2.addr), nil).Once()

	var client, err = NewClient(s0.addr)
	c.Check(err, gc.IsNil)
	defer client.Close()

	c.Check(client.State(), gc.DeepEquals, *buildConsumerStateFixture(s0.addr, s1.addr, s2.addr))

	conn, shard, err := client.PartitionClient("partition/zero")
	c.Check(err, gc.IsNil)
	c.Check(conn, gc.NotNil)
	c.Check(shard.Id, gc.Equals, ShardID("shard-zero"))

	conn, shard, err = client.PartitionClient("partition/one")
	c.Check(err, gc.Equals, ErrNoReadyPartitionClient)
	c.Check(conn, gc.IsNil)
	c.Check(shard.Id, gc.Equals, ShardID("shard-one"))

	conn, shard, err = client.PartitionClient("partition/two")
	c.Check(err, gc.IsNil)
	c.Check(conn, gc.NotNil)
	c.Check(shard.Id, gc.Equals, ShardID("shard-two"))

	conn, shard, err = client.PartitionClient("partition/three")
	c.Check(err, gc.Equals, ErrNoSuchConsumerPartition)
	c.Check(conn, gc.IsNil)

	var s3 = buildMockServer(c)
	defer s3.srv.GracefulStop()

	// Add a new server and define shard-three.
	var fixture = buildConsumerStateFixture(s0.addr, s1.addr, s2.addr)

	fixture.Endpoints = sortStrings([]string{s0.addr, s1.addr, s3.addr})
	fixture.Shards = append(fixture.Shards, ConsumerState_Shard{
		Id:        "shard-three",
		Topic:     "a/topic",
		Partition: "partition/three",
		Replicas: []ConsumerState_Replica{
			{
				Endpoint: s3.addr,
				Status:   ConsumerState_Replica_PRIMARY,
			},
		},
	})
	s0.mock.On("CurrentConsumerState", mock.Anything, &Empty{}).Return(fixture, nil).Once()

	c.Check(client.update(), gc.IsNil)

	// Expect partition/zero is still addressable, and now so is partition/three.
	conn, shard, err = client.PartitionClient("partition/zero")
	c.Check(err, gc.IsNil)
	c.Check(conn, gc.NotNil)
	c.Check(shard.Id, gc.Equals, ShardID("shard-zero"))

	conn, shard, err = client.PartitionClient("partition/three")
	c.Check(err, gc.IsNil)
	c.Check(conn, gc.NotNil)
	c.Check(shard.Id, gc.Equals, ShardID("shard-three"))
}

type mockConsumerServer struct {
	srv  *grpc.Server
	mock *MockConsumerServer
	addr string
}

func buildMockServer(c *gc.C) mockConsumerServer {
	var srv = grpc.NewServer()
	var m = new(MockConsumerServer)

	RegisterConsumerServer(srv, m)

	var l, err = net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, gc.IsNil)

	go srv.Serve(l)

	return mockConsumerServer{
		srv:  srv,
		mock: m,
		addr: l.Addr().String(),
	}
}

func buildConsumerStateFixture(addr0, addr1, addr2 string) *ConsumerState {
	return &ConsumerState{
		Root:          "a/root",
		LocalRouteKey: addr0,
		ReplicaCount:  1,
		Endpoints:     []string{addr0, addr1},
		Shards: []ConsumerState_Shard{
			// shard-zero is PRIMARY and addressable.
			{
				Id:        "shard-zero",
				Topic:     "a/topic",
				Partition: "partition/zero",
				Replicas: []ConsumerState_Replica{
					{
						Endpoint: addr1,
						Status:   ConsumerState_Replica_PRIMARY,
					},
					{
						Endpoint: addr0,
						Status:   ConsumerState_Replica_RECOVERING,
					},
				},
			},
			// shard-one is addressable but not PRIMARY.
			{
				Id:        "shard-one",
				Topic:     "a/topic",
				Partition: "partition/one",
				Replicas: []ConsumerState_Replica{
					{
						Endpoint: addr0,
						Status:   ConsumerState_Replica_RECOVERING,
					},
				},
			},
			// shard-two is PRIMARY and addressable, though not in Endpoints (eg, member is in process of hand-off).
			// It's replica is not accessible (but not connected to, either, as non-PRIMARY).
			{
				Id:        "shard-two",
				Topic:     "a/topic",
				Partition: "partition/two",
				Replicas: []ConsumerState_Replica{
					{
						Endpoint: addr2,
						Status:   ConsumerState_Replica_PRIMARY,
					},
					{
						Endpoint: "[100::1]:1234", // RFC 6666 black-hole IP.
						Status:   ConsumerState_Replica_READY,
					},
				},
			},
		},
	}
}

func sortStrings(s []string) []string {
	sort.Strings(s)
	return s
}

var _ = gc.Suite(&ClientSuite{})
