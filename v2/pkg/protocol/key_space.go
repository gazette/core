package protocol

import (
	"github.com/LiveRamp/gazette/pkg/keyspace"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

// NewKeySpace returns a KeySpace suitable for use with an Allocator.
// It decodes allocator Items as JournalSpec messages, Members as BrokerSpecs,
// and Assignments as Routes.
func NewKeySpace(prefix string) *keyspace.KeySpace {
	return v3_allocator.NewAllocatorKeySpace(prefix, decoder{})
}

// decoder is an instance of v3_allocator.AllocatorDecoder. It strictly enforces
// that JournalSpec.Name and BrokerSpec.ID match those derived from the Spec's
// Etcd key. Conceptually, the Etcd key is source-of-truth regarding entity
// naming, but it's helpful to carry these identifiers within the specs
// themselves, making them a stand-alone representation and allowing for
// content-addressing of a spec into its appropriate Etcd key. This decoder
// behavior provides an assertion that these identifiers never diverge.
type decoder struct{}

func (d decoder) DecodeItem(id string, raw *mvccpb.KeyValue) (v3_allocator.ItemValue, error) {
	var s = new(JournalSpec)

	if err := s.Unmarshal(raw.Value); err != nil {
		return nil, err
	} else if err = s.Validate(); err != nil {
		return nil, err
	} else if s.Name.String() != id {
		return nil, NewValidationError("JournalSpec Name doesn't match Item ID (%+v vs %+v)", s.Name, id)
	}
	return s, nil
}

func (d decoder) DecodeMember(zone, suffix string, raw *mvccpb.KeyValue) (v3_allocator.MemberValue, error) {
	var s = new(BrokerSpec)

	if err := s.Unmarshal(raw.Value); err != nil {
		return nil, err
	} else if err = s.Validate(); err != nil {
		return nil, err
	} else if s.Id.Zone != zone {
		return nil, NewValidationError("BrokerSpec Zone doesn't match Member Zone (%+v vs %+v)", s.Id.Zone, zone)
	} else if s.Id.Suffix != suffix {
		return nil, NewValidationError("BrokerSpec Suffix doesn't match Member Suffix (%+v vs %+v)", s.Id.Suffix, suffix)
	}
	return s, nil
}

func (d decoder) DecodeAssignment(itemID, memberZone, memberSuffix string, slot int, raw *mvccpb.KeyValue) (v3_allocator.AssignmentValue, error) {
	var s = new(Route)

	if len(raw.Value) == 0 {
		s.Init(nil)
	} else if err := s.Unmarshal(raw.Value); err != nil {
		return nil, err
	} else if err = s.Validate(); err != nil {
		return nil, err
	}
	return s, nil
}
