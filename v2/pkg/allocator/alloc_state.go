package v3_allocator

import (
	"hash/crc64"
	"math"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/keyspace"
)

// State is an extracted representation of the allocator KeySpace. Clients may
// want to inspect State as part of a KeySpace observer to identify changes to
// local assignments or the overall allocation topology.
type State struct {
	KS       *keyspace.KeySpace
	LocalKey string // Unique key of this allocator instance.

	// Sub-slices of the KeySpace representing allocator entities.
	Members     keyspace.KeyValues
	Items       keyspace.KeyValues
	Assignments keyspace.KeyValues

	LocalMemberInd int         // Index of |LocalKey| within |Members|, or -1 if not found.
	LocalItems     []LocalItem // Assignments of this instance.

	Zones       []string // Sorted and unique Zones of |Members|.
	MemberSlots int      // Total number of item slots summed across all |Members|.
	ItemSlots   int      // Total desired replication slots summed across all |Items|.
	NetworkHash uint64   // Content-sum which captures Items & Members, and their constraints.

	// Number of total Assignments, and primary Assignments by Member.
	// These share cardinality with |Members|.
	MemberTotalCount   []int
	MemberPrimaryCount []int
}

// NewObservedState returns a *State instance which extracts and updates itself
// from the provided KeySpace, pivoted around the Member instance identified by
// |localKey|. State should be treated as read-only, and a read lock of the
// parent KeySpace must be obtained before each use.
func NewObservedState(ks *keyspace.KeySpace, localKey string) *State {
	var s = &State{
		KS:             ks,
		LocalKey:       localKey,
		LocalMemberInd: -1,
	}
	ks.Mu.Lock()
	ks.Observers = append(ks.Observers, s.observe)
	ks.Mu.Unlock()
	return s
}

// observe extracts a current State representation from the KeySpace,
// pivoted around the Member instance identified by |LocalKey|.
func (s *State) observe() {
	*s = State{
		KS:       s.KS,
		LocalKey: s.LocalKey,

		Members:     s.KS.Prefixed(s.KS.Root + MembersPrefix),
		Items:       s.KS.Prefixed(s.KS.Root + ItemsPrefix),
		Assignments: s.KS.Prefixed(s.KS.Root + AssignmentsPrefix),
	}
	s.MemberTotalCount = make([]int, len(s.Members))
	s.MemberPrimaryCount = make([]int, len(s.Members))

	// Walk Members to:
	//  * Group the set of ordered |Zones| across all Members.
	//  * Initialize |MemberSlots|.
	//  * Initialize |NetworkHash|.
	for i := range s.Members {
		var m = memberAt(s.Members, i)
		var l = m.ItemLimit()

		if len(s.Zones) == 0 {
			s.Zones = append(s.Zones, m.Zone)
		} else if z := s.Zones[len(s.Zones)-1]; z < m.Zone {
			s.Zones = append(s.Zones, m.Zone)
		} else if z > m.Zone {
			panic("invalid Member order")
		}

		s.MemberSlots += l
		s.NetworkHash = foldCRC(s.NetworkHash, s.Members[i].Raw.Key, l)
	}

	// Fetch |localMember| identified by |LocalKey|.
	if ind, found := s.Members.Search(s.LocalKey); !found {
		s.LocalMemberInd = -1
	} else {
		s.LocalMemberInd = ind
	}

	// Left-join Items with their Assignments to:
	//   * Initialize |ItemSlots|.
	//   * Initialize |NetworkHash|.
	//   * Collect Items and Assignments which map to the |LocalKey| Member.
	//   * Accumulate per-Member counts of primary and total Assignments.
	var it = leftJoin{
		lenL: len(s.Items),
		lenR: len(s.Assignments),
		compare: func(l, r int) int {
			return strings.Compare(itemAt(s.Items, l).ID, assignmentAt(s.Assignments, r).ItemID)
		},
	}
	for cur, ok := it.next(); ok; cur, ok = it.next() {
		var item = itemAt(s.Items, cur.left)
		var r = item.DesiredReplication()

		s.ItemSlots += r
		s.NetworkHash = foldCRC(s.NetworkHash, s.Items[cur.left].Raw.Key, r)

		for r := cur.rightBegin; r != cur.rightEnd; r++ {
			var a = assignmentAt(s.Assignments, r)
			var key = MemberKey(s.KS, a.MemberZone, a.MemberSuffix)

			if key == s.LocalKey {
				s.LocalItems = append(s.LocalItems, LocalItem{
					Item:        s.Items[cur.left],
					Assignments: s.Assignments[cur.rightBegin:cur.rightEnd],
					Index:       r - cur.rightBegin,
				})
			}
			if ind, found := s.Members.Search(key); found {
				if a.Slot == 0 {
					s.MemberPrimaryCount[ind]++
				}
				s.MemberTotalCount[ind]++
			}
		}
	}
}

// shouldExit returns true iff the local Member is able to safely exit.
func (s *State) shouldExit() bool {
	return memberAt(s.Members, s.LocalMemberInd).ItemLimit() == 0 && len(s.LocalItems) == 0
}

// isLeader returns true iff the local Member key is ordered first on
// (CreateRevision, Key) among all Member keys.
func (s *State) isLeader() bool {
	var leader keyspace.KeyValue
	for _, kv := range s.Members {
		if leader.Raw.CreateRevision == 0 || kv.Raw.CreateRevision < leader.Raw.CreateRevision {
			leader = kv
		}
	}
	return string(leader.Raw.Key) == s.LocalKey
}

func (s *State) debugLog() {
	var la []string
	for _, a := range s.LocalItems {
		la = append(la, string(a.Assignments[a.Index].Raw.Key))
	}

	log.WithFields(log.Fields{
		"Assignments":    len(s.Assignments),
		"ItemSlots":      s.ItemSlots,
		"Items":          len(s.Items),
		"LocalItems":     len(la),
		"LocalKey":       s.LocalKey,
		"LocalMemberInd": s.LocalMemberInd,
		"MemberSlots":    s.MemberSlots,
		"Members":        len(s.Members),
		"NetworkHash":    s.NetworkHash,
		"Revision":       s.KS.Header.Revision,
		"Zones":          s.Zones,
	}).Info("extracted State")
}

// memberLoadRatio maps an |assignment| to a Member "load ratio". Given all
// |Members| and their corresponding |counts| (1:1 with |Members|),
// memberLoadRatio maps |assignment| to a Member and, if found, returns the
// ratio of the Member's index in |counts| to the Member's ItemLimit. If the
// Member is not found, infinity is returned.
func (s *State) memberLoadRatio(assignment keyspace.KeyValue, counts []int) float32 {
	var a = assignment.Decoded.(Assignment)

	if ind, found := s.Members.Search(MemberKey(s.KS, a.MemberZone, a.MemberSuffix)); found {
		return float32(counts[ind]) / float32(memberAt(s.Members, ind).ItemLimit())
	}
	return math.MaxFloat32
}

func foldCRC(crc uint64, key []byte, n int) uint64 {
	var tmp [12]byte
	crc = crc64.Update(crc, crcTable, key)
	crc = crc64.Update(crc, crcTable, strconv.AppendInt(tmp[:0], int64(n), 10))
	return crc
}

var crcTable = crc64.MakeTable(crc64.ECMA)
