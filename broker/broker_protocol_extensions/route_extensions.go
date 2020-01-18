package broker_protocol_extensions

import (
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/keyspace"
)

// BrokerRoute extends protocol.Route allowing initialization from allocator assignments
// and attaching endpoints from a keyspace
type BrokerRoute struct {
	Route pb.Route
}

// Init Initializes Route with the provided allocator Assignments.
func (m *BrokerRoute) Init(assignments keyspace.KeyValues) {
	*m = BrokerRoute{Route: pb.Route{Primary: -1, Members: m.Route.Members[:0]}}

	for _, kv := range assignments {
		var a = kv.Decoded.(allocator.Assignment)
		if a.Slot == 0 {
			m.Route.Primary = int32(len(m.Route.Members))
		}

		m.Route.Members = append(m.Route.Members, pb.ProcessSpec_ID{
			Zone:   a.MemberZone,
			Suffix: a.MemberSuffix,
		})
	}
}

// AttachEndpoints maps Route members through the KeySpace to their respective
// specs, and attaches the associated Endpoint of each to the Route.
// KeySpace must already be read-locked.
func (m *BrokerRoute) AttachEndpoints(ks *keyspace.KeySpace) {
	if len(m.Route.Members) != 0 {
		m.Route.Endpoints = make([]pb.Endpoint, len(m.Route.Members))
	}
	for i, b := range m.Route.Members {
		if member, ok := allocator.LookupMember(ks, b.Zone, b.Suffix); !ok {
			continue // Assignment with missing Member. Ignore.
		} else {
			m.Route.Endpoints[i] = member.MemberValue.(interface {
				GetEndpoint() pb.Endpoint
			}).GetEndpoint()
		}
	}
}

func (m BrokerRoute) Equivalent(other *BrokerRoute) bool {
	return m.Route.Equivalent(&other.Route)
}
