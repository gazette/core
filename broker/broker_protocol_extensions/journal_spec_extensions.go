package broker_protocol_extensions

import (
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/keyspace"
)

type BrokerJournalSpec struct {
	JournalSpec pb.JournalSpec
}

// IsConsistent returns true if the Route stored under each of |assignments|
// agrees with the Route implied by the |assignments| keys. It implements
// allocator.ItemValue.
func (m *BrokerJournalSpec) IsConsistent(_ keyspace.KeyValue, assignments keyspace.KeyValues) bool {
	var rt BrokerRoute
	rt.Init(assignments)

	return JournalRouteMatchesAssignments(rt, assignments)
}

// DesiredReplication returns the configured Replication of the spec. It
// implements allocator.ItemValue.
func (m *BrokerJournalSpec) DesiredReplication() int { return int(m.JournalSpec.Replication) }

// JournalRouteMatchesAssignments returns true iff the Route is equivalent to the
// Route marshaled with each of the journal's |assignments|.
func JournalRouteMatchesAssignments(rt BrokerRoute, assignments keyspace.KeyValues) bool {
	for _, a := range assignments {
		if !rt.Route.Equivalent(a.Decoded.(allocator.Assignment).AssignmentValue.(*pb.Route)) {
			return false
		}
	}
	return len(rt.Route.Members) == len(assignments)
}
