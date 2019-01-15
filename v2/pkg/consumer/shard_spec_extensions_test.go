package consumer

import (
	"testing"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

type SpecSuite struct{}

func (s *SpecSuite) TestShardValidationCases(c *gc.C) {
	var cases = []struct {
		s      ShardID
		expect string
	}{
		{"a-valid-shard", ""}, // Success.
		{"not-$%|-a-token", `not a valid token \(.*\)`},
		{"", `invalid length \(0; expected 4 <= .*`},
		{"zz", `invalid length \(2; expected 4 <= .*`},
	}
	for _, tc := range cases {
		if tc.expect == "" {
			c.Check(tc.s.Validate(), gc.IsNil)
		} else {
			c.Check(tc.s.Validate(), gc.ErrorMatches, tc.expect)
		}
	}
}

func (s *SpecSuite) TestShardSpecValidationCases(c *gc.C) {
	var spec = ShardSpec{
		Id:             "bad id",
		Sources:        nil,
		RecoveryLog:    "bad log",
		HintKeys:       nil,
		MaxTxnDuration: +0,
		MinTxnDuration: -1,
		LabelSet:       pb.LabelSet{Labels: []pb.Label{{Name: "bad label", Value: "value"}}},
	}

	c.Check(spec.Validate(), gc.ErrorMatches, `Id: not a valid token \(bad id\)`)
	spec.Id = "a-shard-id"
	c.Check(spec.Validate(), gc.ErrorMatches, `Sources cannot be empty`)
	spec.Sources = []ShardSpec_Source{
		{Journal: "journal 2"},
		{Journal: "journal/1", MinOffset: -1},
	}
	c.Check(spec.Validate(), gc.ErrorMatches, `RecoveryLog: not a valid token \(bad log\)`)
	spec.RecoveryLog = "a/recovery/log"
	c.Check(spec.Validate(), gc.ErrorMatches, `HintKeys cannot be empty`)
	spec.HintKeys = []string{""}
	c.Check(spec.Validate(), gc.ErrorMatches, `invalid MinTxnDuration \(-1; expected >= 0\)`)
	spec.MinTxnDuration = 0
	c.Check(spec.Validate(), gc.ErrorMatches, `invalid MaxTxnDuration \(0; expected > 0\)`)
	spec.MaxTxnDuration = 1
	c.Check(spec.Validate(), gc.ErrorMatches, `LabelSet.Labels\[0\].Name: not a valid token \(bad label\)`)
	spec.Labels[0].Name = "label"

	c.Check(spec.Validate(), gc.ErrorMatches, `Sources\[0\].Journal: not a valid token \(journal 2\)`)
	spec.Sources[0].Journal = "journal/2"
	c.Check(spec.Validate(), gc.ErrorMatches, `Sources\[1\]: invalid MinOffset \(-1; expected > 0\)`)
	spec.Sources[1].MinOffset = 1024
	c.Check(spec.Validate(), gc.ErrorMatches, `Sources.Journal not in unique, sorted order \(index 1; journal/1 <= journal/2\)`)
	spec.Sources[0], spec.Sources[1] = spec.Sources[1], spec.Sources[0]

	c.Check(spec.Validate(), gc.ErrorMatches, `HintKeys\[0\] is not an absolute, clean, non-directory path \(\)`)
	spec.HintKeys[0] = "relative/path"
	c.Check(spec.Validate(), gc.ErrorMatches, `HintKeys\[0\] is not an absolute, clean, non-directory path \(relative/path\)`)
	spec.HintKeys[0] = "/rooted/dir/"
	c.Check(spec.Validate(), gc.ErrorMatches, `HintKeys\[0\] is not an absolute, clean, non-directory path \(/rooted/dir/\)`)
	spec.HintKeys[0] = "/rooted//path" // Not clean.
	c.Check(spec.Validate(), gc.ErrorMatches, `HintKeys\[0\] is not an absolute, clean, non-directory path \(/rooted//path\)`)
	spec.HintKeys[0] = "/rooted/path"

	c.Check(spec.Validate(), gc.IsNil)
}

func (s *SpecSuite) TestShardSpecRoutines(c *gc.C) {
	var spec = ShardSpec{
		Id:          "shard-id",
		HotStandbys: 2,
		Disable:     false,
	}
	c.Check(spec.DesiredReplication(), gc.Equals, 3)
	spec.Disable = true
	c.Check(spec.DesiredReplication(), gc.Equals, 0)
	spec.Disable, spec.HotStandbys = false, 0
	c.Check(spec.DesiredReplication(), gc.Equals, 1)

	var status = new(ReplicaStatus)
	var asn = keyspace.KeyValue{Decoded: allocator.Assignment{AssignmentValue: status}}

	c.Check(spec.IsConsistent(asn, nil), gc.Equals, false)
	status.Code = ReplicaStatus_TAILING
	c.Check(spec.IsConsistent(asn, nil), gc.Equals, true)

	c.Check(ExtractShardSpecMetaLabels(&spec, pb.MustLabelSet("label", "buffer")),
		gc.DeepEquals, pb.MustLabelSet("id", "shard-id"))
}

func (s *SpecSuite) TestConsumerSpecValidationCases(c *gc.C) {
	var spec = ConsumerSpec{
		ProcessSpec: pb.ProcessSpec{
			Id:       pb.ProcessSpec_ID{Zone: "not valid", Suffix: "name"},
			Endpoint: "http://foo",
		},
		ShardLimit: 5,
	}
	c.Check(spec.Validate(), gc.ErrorMatches, `Id.Zone: not a valid token \(not valid\)`)
	spec.Id.Zone = "zone"

	c.Check(spec.Validate(), gc.IsNil)
	c.Check(spec.ItemLimit(), gc.Equals, 5)
}

func (s *SpecSuite) TestReplicaStatusValidationCases(c *gc.C) {
	var status = ReplicaStatus{Code: -1}

	c.Check(status.Validate(), gc.ErrorMatches, `Code: invalid code \(-1\)`)
	status.Code = ReplicaStatus_TAILING
	c.Check(status.Validate(), gc.IsNil)

	status.Errors = []string{"error!"}
	c.Check(status.Validate(), gc.ErrorMatches, `expected Code FAILED with non-empty Errors`)
	status.Errors, status.Code = nil, ReplicaStatus_FAILED
	c.Check(status.Validate(), gc.ErrorMatches, `expected non-empty Errors with Code FAILED`)

	status.Errors = []string{"error!"}
	c.Check(status.Validate(), gc.IsNil)
}

func (s *SpecSuite) TestReplicaStatusReduction(c *gc.C) {
	var status = &ReplicaStatus{Code: ReplicaStatus_IDLE}

	// Code is increased (only) by reducing with a ReplicaStatus of greater Code value.
	status.Reduce(&ReplicaStatus{Code: ReplicaStatus_TAILING})
	c.Check(status, gc.DeepEquals, &ReplicaStatus{Code: ReplicaStatus_TAILING})
	status.Reduce(&ReplicaStatus{Code: ReplicaStatus_BACKFILL})
	c.Check(status, gc.DeepEquals, &ReplicaStatus{Code: ReplicaStatus_TAILING})

	// Multiple errors are accumulated.
	status.Reduce(&ReplicaStatus{Code: ReplicaStatus_FAILED, Errors: []string{"err-1"}})
	status.Reduce(&ReplicaStatus{Code: ReplicaStatus_FAILED, Errors: []string{"err-2"}})
	c.Check(status, gc.DeepEquals, &ReplicaStatus{Code: ReplicaStatus_FAILED, Errors: []string{"err-1", "err-2"}})
}

func (s *SpecSuite) TestStatRequestValidationCases(c *gc.C) {
	var req = StatRequest{
		Header: badHeaderFixture(),
		Shard:  "invalid shard",
	}
	c.Check(req.Validate(), gc.ErrorMatches, `Header.Etcd: invalid ClusterId .*`)
	req.Header.Etcd.ClusterId = 1234
	c.Check(req.Validate(), gc.ErrorMatches, `Shard: not a valid token \(invalid shard\)`)
	req.Shard = "valid-shard"

	c.Check(req.Validate(), gc.IsNil)
}

func (s *SpecSuite) TestStatResponseValidationCases(c *gc.C) {
	var resp = StatResponse{
		Status: 9101,
		Header: *badHeaderFixture(),
		Offsets: map[pb.Journal]int64{
			"invalid journal": 123,
		},
	}
	c.Check(resp.Validate(), gc.ErrorMatches, `Status: invalid status \(9101\)`)
	resp.Status = Status_OK
	c.Check(resp.Validate(), gc.ErrorMatches, `Header.Etcd: invalid ClusterId .*`)
	resp.Header.Etcd.ClusterId = 1234
	c.Check(resp.Validate(), gc.ErrorMatches, `Offsets\[invalid journal\]: not a valid token \(invalid journal\)`)
	delete(resp.Offsets, "invalid journal")
	resp.Offsets["a/journal"] = -456
	c.Check(resp.Validate(), gc.ErrorMatches, `Offsets\[a/journal\]: invalid offset \(-456; expected >= 0\)`)
	resp.Offsets["a/journal"] = 789

	c.Check(resp.Validate(), gc.IsNil)
}

func (s *SpecSuite) TestListRequestValidationCases(c *gc.C) {
	var req = ListRequest{
		Selector: pb.LabelSelector{
			Include: pb.LabelSet{Labels: []pb.Label{{Name: "a invalid name", Value: "foo"}}},
			Exclude: pb.MustLabelSet("id", "bar"),
		},
	}
	c.Check(req.Validate(), gc.ErrorMatches,
		`Selector.Include.Labels\[0\].Name: not a valid token \(a invalid name\)`)
	req.Selector.Include.Labels[0].Name = "a-valid-name"

	c.Check(req.Validate(), gc.IsNil)
}

func (s *SpecSuite) TestListResponseValidationCases(c *gc.C) {
	var resp = ListResponse{
		Status: 9101,
		Header: *badHeaderFixture(),
		Shards: []ListResponse_Shard{
			{
				ModRevision: 0,
				Spec: ShardSpec{
					Id:             "a invalid id",
					Sources:        []ShardSpec_Source{{Journal: "a/journal"}},
					RecoveryLog:    "a/log",
					HintKeys:       []string{"/a/hint/key"},
					MaxTxnDuration: 1,
				},
				Route:  pb.Route{Primary: 0},
				Status: nil,
			},
		},
	}

	c.Check(resp.Validate(), gc.ErrorMatches, `Status: invalid status \(9101\)`)
	resp.Status = Status_OK
	c.Check(resp.Validate(), gc.ErrorMatches, `Header.Etcd: invalid ClusterId .*`)
	resp.Header.Etcd.ClusterId = 1234
	c.Check(resp.Validate(), gc.ErrorMatches, `Shards\[0\].Spec.Id: not a valid token \(.*\)`)
	resp.Shards[0].Spec.Id = "a-valid-id"
	c.Check(resp.Validate(), gc.ErrorMatches, `Shards\[0\]: invalid ModRevision \(0; expected > 0\)`)
	resp.Shards[0].ModRevision = 1
	c.Check(resp.Validate(), gc.ErrorMatches, `Shards\[0\].Route: invalid Primary .*`)
	resp.Shards[0].Route.Members = []pb.ProcessSpec_ID{{Zone: "zone", Suffix: "suffix"}}
	c.Check(resp.Validate(), gc.ErrorMatches, `Shards\[0\]: length of Route.Members and Status are not equal \(1 vs 0\)`)
	resp.Shards[0].Status = []ReplicaStatus{{Code: ReplicaStatus_TAILING}}

	c.Check(resp.Validate(), gc.IsNil)
}

func (s *SpecSuite) TestApplyRequestValidationCases(c *gc.C) {
	var req = ApplyRequest{
		Changes: []ApplyRequest_Change{
			{
				ExpectModRevision: -1,
				Upsert: &ShardSpec{
					Id:             "invalid id",
					Sources:        []ShardSpec_Source{{Journal: "a/journal"}},
					RecoveryLog:    "a/log",
					HintKeys:       []string{"/a/hint/key"},
					MaxTxnDuration: 1,
				},
				Delete: "another-id",
			},
			{
				ExpectModRevision: 0,
				Delete:            "another invalid id",
			},
			{
				ExpectModRevision: 1,
			},
		},
	}

	c.Check(req.Validate(), gc.ErrorMatches, `Changes\[0\]: both Upsert and Delete are set \(expected exactly one\)`)
	req.Changes[0].Delete = ""
	c.Check(req.Validate(), gc.ErrorMatches, `Changes\[0\].Upsert.Id: not a valid token \(invalid id\)`)
	req.Changes[0].Upsert.Id = "a-valid-id"
	c.Check(req.Validate(), gc.ErrorMatches, `Changes\[0\]: invalid ExpectModRevision \(-1; expected >= 0\)`)
	req.Changes[0].ExpectModRevision = 0
	c.Check(req.Validate(), gc.ErrorMatches, `Changes\[1\].Delete: not a valid token \(another invalid id\)`)
	req.Changes[1].Delete = "other-valid-id"
	c.Check(req.Validate(), gc.ErrorMatches, `Changes\[1\]: invalid ExpectModRevision \(0; expected > 0\)`)
	req.Changes[1].ExpectModRevision = 1
	c.Check(req.Validate(), gc.ErrorMatches, `Changes\[2\]: neither Upsert nor Delete are set \(expected exactly one\)`)
	req.Changes[2].Delete = "yet-another-valid-id"

	c.Check(req.Validate(), gc.IsNil)
}

func (s *SpecSuite) TestApplyResponseValidationCases(c *gc.C) {
	var resp = ApplyResponse{
		Status: 9101,
		Header: *badHeaderFixture(),
	}

	c.Check(resp.Validate(), gc.ErrorMatches, `Status: invalid status \(9101\)`)
	resp.Status = Status_OK
	c.Check(resp.Validate(), gc.ErrorMatches, `Header.Etcd: invalid ClusterId .*`)
	resp.Header.Etcd.ClusterId = 1234

	c.Check(resp.Validate(), gc.IsNil)
}

func badHeaderFixture() *pb.Header {
	return &pb.Header{
		ProcessId: pb.ProcessSpec_ID{Zone: "zone", Suffix: "name"},
		Route:     pb.Route{Primary: 0, Members: []pb.ProcessSpec_ID{{Zone: "zone", Suffix: "name"}}},
		Etcd: pb.Header_Etcd{
			ClusterId: 0, // ClusterId is invalid, but easily fixed up.
			MemberId:  34,
			Revision:  56,
			RaftTerm:  78,
		},
	}
}

var _ = gc.Suite(&SpecSuite{})

func TestT(t *testing.T) { gc.TestingT(t) }
