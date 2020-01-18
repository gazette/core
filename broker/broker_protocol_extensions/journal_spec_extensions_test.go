package broker_protocol_extensions

import (
	gc "github.com/go-check/check"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/keyspace"
)

type JournalSuite struct{}

func (s *JournalSuite) TestConsistencyCases(c *gc.C) {
	var routes [3]pb.Route
	var assignments keyspace.KeyValues

	for i := range routes {
		routes[i].Primary = 0
		routes[i].Members = []pb.ProcessSpec_ID{
			{Zone: "zone/a", Suffix: "member/1"},
			{Zone: "zone/a", Suffix: "member/3"},
			{Zone: "zone/b", Suffix: "member/2"},
		}
		assignments = append(assignments, keyspace.KeyValue{
			Decoded: allocator.Assignment{
				AssignmentValue: &routes[i],
				MemberSuffix:    routes[0].Members[i].Suffix,
				MemberZone:      routes[0].Members[i].Zone,
				Slot:            i,
			},
		})
	}

	var spec BrokerJournalSpec
	c.Check(spec.IsConsistent(keyspace.KeyValue{}, assignments), gc.Equals, true)

	routes[0].Primary = 1
	c.Check(spec.IsConsistent(keyspace.KeyValue{}, assignments), gc.Equals, false)
	routes[0].Primary = 0

	routes[1].Members = append(routes[1].Members, pb.ProcessSpec_ID{Zone: "zone/b", Suffix: "member/4"})
	c.Check(spec.IsConsistent(keyspace.KeyValue{}, assignments), gc.Equals, false)
}
