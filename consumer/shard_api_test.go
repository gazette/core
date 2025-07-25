package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
)

func TestAPIStatCases(t *testing.T) {
	var tf, cleanup = newTestFixture(t)
	defer cleanup()

	tf.allocateShard(makeShard(shardA), localID)
	expectStatusCode(t, tf.state, pc.ReplicaStatus_PRIMARY)

	// Publish to |sourceB|.
	var aa, _ = tf.pub.PublishCommitted(toSourceB, &testMessage{Key: "read", Value: "through"})
	<-aa.Done()

	// Case: Stat local shard. Expect it to have read-through our publish.
	resp, err := tf.service.Stat(context.Background(), allClaims,
		&pc.StatRequest{
			Shard:       shardA,
			ReadThrough: pb.Offsets{sourceB.Name: aa.Response().Commit.End},
		})
	require.NoError(t, err)
	require.Equal(t, pc.Status_OK, resp.Status)
	require.Equal(t, map[pb.Journal]int64{
		sourceA.Name: 0,
		sourceB.Name: aa.Response().Commit.End,
	}, resp.ReadThrough)
	require.Equal(t, localID, resp.Header.ProcessId)

	// Case: Stat of non-existent Shard.
	resp, err = tf.service.Stat(context.Background(), allClaims,
		&pc.StatRequest{Shard: "missing-shard"})
	require.NoError(t, err)
	require.Equal(t, pc.Status_SHARD_NOT_FOUND, resp.Status)

	// Case: Insufficient claimed selector.
	resp, err = tf.service.Stat(context.Background(), noClaims,
		&pc.StatRequest{Shard: shardA})
	require.NoError(t, err)
	require.Equal(t, pc.Status_SHARD_NOT_FOUND, resp.Status) // Shard not visible to these claims.
	require.Len(t, resp.Header.Route.Endpoints, 0)

	// TODO(johnny): Proxy case ought to be unit-tested here.
	// Adding it is currently low-value because it's covered by other E2E tests
	// and newTestFixture isn't set up for verifying proxy interactions.

	tf.allocateShard(makeShard(shardA)) // Cleanup.
}

func TestAPIListCases(t *testing.T) {
	var tf, cleanup = newTestFixture(t)
	defer cleanup()

	var specA = makeShard(shardA)
	specA.Labels = append(specA.Labels, pb.Label{Name: "foo", Value: "bar"})
	var specB = makeShard(shardB)
	specB.Labels = append(specB.Labels, pb.Label{Name: "bar", Value: "baz"})
	var specC = makeShard(shardC)

	tf.allocateShard(specA)
	tf.allocateShard(specB, remoteID)
	tf.allocateShard(specC, localID, remoteID)
	expectStatusCode(t, tf.state, pc.ReplicaStatus_PRIMARY)

	var verify = func(resp *pc.ListResponse, expect ...*pc.ShardSpec) {
		require.Equal(t, pc.Status_OK, resp.Status)
		require.Len(t, resp.Shards, len(expect))

		for i, exp := range expect {
			require.Equal(t, *exp, resp.Shards[i].Spec)

			require.NotZero(t, resp.Shards[i].ModRevision)
			require.NotZero(t, resp.Shards[i].CreateRevision)

			var numAsn int
			if exp == specB {
				numAsn = 1
			} else if exp == specC {
				numAsn = 2
			}
			require.Len(t, resp.Shards[i].Status, numAsn)
			require.Len(t, resp.Shards[i].Route.Endpoints, numAsn)
		}
	}

	// Case: Empty selector returns all shards.
	var resp, err = tf.service.List(context.Background(), allClaims, &pc.ListRequest{
		Selector: pb.LabelSelector{},
	})
	require.NoError(t, err)
	verify(resp, specA, specB, specC)

	// Expect current Etcd-backed status is returned with each shard.
	require.Equal(t, pc.ReplicaStatus_PRIMARY, resp.Shards[2].Status[0].Code)

	// Case: Exclude on label.
	resp, err = tf.service.List(context.Background(), allClaims, &pc.ListRequest{
		Selector: pb.LabelSelector{Exclude: pb.MustLabelSet("foo", "")},
	})
	require.NoError(t, err)
	verify(resp, specB, specC)

	// Case: Meta-label "id" selects specific shards.
	resp, err = tf.service.List(context.Background(), allClaims, &pc.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("id", shardC)},
	})
	require.NoError(t, err)
	verify(resp, specC)

	// Case: Claims scope the visibile shards.
	resp, err = tf.service.List(context.Background(),
		pb.Claims{
			Capability: pb.Capability_LIST,
			Selector:   pb.MustLabelSelector("foo"),
		},
		&pc.ListRequest{Selector: pb.LabelSelector{}})
	require.NoError(t, err)
	verify(resp, specA)

	resp, err = tf.service.List(context.Background(), noClaims,
		&pc.ListRequest{Selector: pb.LabelSelector{}})
	require.NoError(t, err)
	verify(resp)

	// Case: Errors on request validation error.
	_, err = tf.service.List(context.Background(), allClaims, &pc.ListRequest{
		Selector: pb.LabelSelector{Include: pb.LabelSet{Labels: []pb.Label{{Name: "invalid label"}}}},
	})
	require.EqualError(t, err, `Selector.Include.Labels[0].Name: not a valid token (invalid label)`)

	tf.allocateShard(specB) // Cleanup.
	tf.allocateShard(specC)
}

func TestAPIApplyCases(t *testing.T) {
	var tf, cleanup = newTestFixture(t)
	defer cleanup()

	var ctx = context.Background()
	var specA = makeShard(shardA)
	var specB = makeShard(shardB)

	var verifyAndFetchRev = func(id pc.ShardID, expect pc.ShardSpec) int64 {
		var resp, err = tf.service.List(ctx, allClaims, &pc.ListRequest{
			Selector: pb.LabelSelector{Include: pb.MustLabelSet("id", id.String())},
		})
		require.NoError(t, err)
		require.Equal(t, pc.Status_OK, resp.Status)
		require.Equal(t, expect, resp.Shards[0].Spec)
		return resp.Shards[0].ModRevision
	}
	var apply = func(req *pc.ApplyRequest) *pc.ApplyResponse {
		var resp, err = tf.service.Apply(ctx, allClaims, req)
		require.NoError(t, err)
		return resp
	}

	// Case: Create new specs A & B.
	require.Equal(t, pc.Status_OK, apply(&pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{
			{Upsert: specA},
			{Upsert: specB, PrimaryHints: &recoverylog.FSMHints{
				Log:        specB.RecoveryLog(),
				Properties: []recoverylog.Property{{Path: "hello", Content: "shard"}}}},
		},
	}).Status)

	var hints, err = tf.service.GetHints(ctx, allClaims, &pc.GetHintsRequest{Shard: shardB})
	require.NoError(t, err)
	require.Equal(t, recoverylog.Property{Path: "hello", Content: "shard"},
		hints.PrimaryHints.Hints.Properties[0])

	// Case: Update existing spec B.
	var origSpecB = *specB
	specB.Labels = append(specB.Labels, pb.Label{Name: "foo", Value: "bar"})

	require.Equal(t, pc.Status_OK, apply(&pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{
			{Upsert: specB, ExpectModRevision: verifyAndFetchRev(shardB, origSpecB)},
		},
	}).Status)

	// Case: Delete existing spec A.
	require.Equal(t, pc.Status_OK, apply(&pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{
			{Delete: shardA, ExpectModRevision: verifyAndFetchRev(shardA, *specA)},
		},
	}).Status)

	// Case: Deletion at wrong revision fails.
	require.Equal(t, pc.Status_ETCD_TRANSACTION_FAILED, apply(&pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{
			{Delete: shardB, ExpectModRevision: verifyAndFetchRev(shardB, *specB) - 1},
		},
	}).Status)

	// Case: Update at wrong revision fails.
	require.Equal(t, pc.Status_ETCD_TRANSACTION_FAILED, apply(&pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{
			{Upsert: specB, ExpectModRevision: verifyAndFetchRev(shardB, *specB) - 1},
		},
	}).Status)

	// Case: Update with explicit revision of -1 succeeds.
	require.Equal(t, pc.Status_OK, apply(&pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{
			{Upsert: specB, ExpectModRevision: -1},
		},
	}).Status)

	// Case: Deletion with explicit revision of -1 succeeds.
	require.Equal(t, pc.Status_OK, apply(&pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{
			{Delete: shardB, ExpectModRevision: -1},
		},
	}).Status)

	// Case: Insufficient claimed selector on delete.
	_, err = tf.service.Apply(ctx, noClaims, &pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{{Delete: "shard-A", ExpectModRevision: -1}},
	})
	require.EqualError(t, err, `rpc error: code = Unauthenticated desc = not authorized to shard-A`)

	// Case: Insufficient claimed selector on upsert.
	_, err = tf.service.Apply(ctx, noClaims, &pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{{Upsert: specB}},
	})
	require.EqualError(t, err, `rpc error: code = Unauthenticated desc = not authorized to shard-B`)

	// Case: Upsert with labels requires claims to match all labels (not just id).
	// Create claims that require env=staging
	var claimsRequireStagingEnv = pb.Claims{
		Capability: pb.Capability_APPLY,
		Selector:   pb.MustLabelSelector("id=shard-with-labels,env=staging"),
	}
	var specWithLabels = &pc.ShardSpec{
		Id:                "shard-with-labels", // This matches the id in claims
		Sources:           []pc.ShardSpec_Source{{Journal: sourceA.Name}},
		RecoveryLogPrefix: aRecoveryLogPrefix,
		HintPrefix:        "/hints",
		MaxTxnDuration:    time.Second,
		LabelSet:          pb.MustLabelSet("env", "production"), // But env=production doesn't match env=staging
	}
	_, err = tf.service.Apply(ctx, claimsRequireStagingEnv, &pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{{Upsert: specWithLabels}},
	})
	require.EqualError(t, err, `rpc error: code = Unauthenticated desc = not authorized to shard-with-labels`)

	// Case: Upsert succeeds when claims selector matches all labels.
	var ctxMatchingClaims = pb.Claims{
		Capability: pb.Capability_APPLY,
		Selector:   pb.MustLabelSelector("id=shard-with-labels,env=production"),
	}
	resp, err := tf.service.Apply(ctx, ctxMatchingClaims, &pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{{Upsert: specWithLabels}},
	})
	require.NoError(t, err)
	require.Equal(t, pc.Status_OK, resp.Status)

	// Case: Delete still only requires id to match (not other labels).
	var ctxIdOnlyClaims = pb.Claims{
		Capability: pb.Capability_APPLY,
		Selector:   pb.MustLabelSelector("id=shard-with-labels"),
	}
	resp, err = tf.service.Apply(ctx, ctxIdOnlyClaims, &pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{{Delete: "shard-with-labels", ExpectModRevision: -1}},
	})
	require.NoError(t, err)
	require.Equal(t, pc.Status_OK, resp.Status)

	// Case: Invalid requests fail with an error.
	_, err = tf.service.Apply(ctx, allClaims, &pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{{Delete: "invalid shard id"}},
	})
	require.EqualError(t, err, `Changes[0].Delete: not a valid token (invalid shard id)`)
}

func TestAPIApplyShardsInBatches(t *testing.T) {
	var ss = newShardServerStub(t)
	defer ss.cleanup()

	var client = pc.NewRoutedShardClient(ss.client(), pb.NoopDispatchRouter{})
	var hdr = &pb.Header{
		ProcessId: pb.ProcessSpec_ID{Zone: "a", Suffix: "broker"},
		Route: pb.Route{
			Members:   []pb.ProcessSpec_ID{{Zone: "a", Suffix: "broker"}},
			Endpoints: []pb.Endpoint{ss.endpoint()},
			Primary:   0,
		},
		Etcd: pb.Header_Etcd{
			ClusterId: 12,
			MemberId:  34,
			Revision:  56,
			RaftTerm:  78,
		},
	}
	var fixture = &pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{
			{Upsert: makeShard(shardA), ExpectModRevision: 1},
			{Upsert: makeShard(shardB), ExpectModRevision: 1},
		},
	}

	// Case: all changes are submitted in one batch.
	ss.ApplyFunc = func(ctx context.Context, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
		require.Equal(t, fixture, req)
		return &pc.ApplyResponse{Status: pc.Status_OK, Header: *hdr}, nil
	}
	var resp, err = ApplyShards(context.Background(), client, fixture)
	require.NoError(t, err)
	require.Equal(t, &pc.ApplyResponse{Status: pc.Status_OK, Header: *hdr}, resp)

	// Case: size == len(req.Changes).
	resp, err = ApplyShardsInBatches(context.Background(), client, fixture, 2)
	require.NoError(t, err)
	require.Equal(t, &pc.ApplyResponse{Status: pc.Status_OK, Header: *hdr}, resp)

	// Case: size < len(req.Changes). Changes are batched.
	var iter = 0
	ss.ApplyFunc = func(ctx context.Context, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
		require.Equal(t, &pc.ApplyRequest{
			Changes: []pc.ApplyRequest_Change{
				{Upsert: fixture.Changes[iter].Upsert, ExpectModRevision: 1},
			},
		}, req)
		iter++
		return &pc.ApplyResponse{Status: pc.Status_OK, Header: *hdr}, nil
	}
	resp, err = ApplyShardsInBatches(context.Background(), client, fixture, 1)
	require.NoError(t, err)
	require.Equal(t, &pc.ApplyResponse{Status: pc.Status_OK, Header: *hdr}, resp)
}

func TestAPIHintsCases(t *testing.T) {
	var tf, cleanup = newTestFixture(t)
	defer cleanup()

	// Install but don't allocate the shard (as recovery will insert hints,
	// and we don't want any yet).
	var spec = makeShard(shardA)
	tf.allocateShard(spec)

	// Case: No hints exist.
	var resp, err = tf.service.GetHints(context.Background(), allClaims, &pc.GetHintsRequest{Shard: shardA})
	require.NoError(t, err)
	require.Equal(t, &pc.GetHintsResponse{
		Status:       pc.Status_OK,
		Header:       resp.Header,
		PrimaryHints: pc.GetHintsResponse_ResponseHints{},
		BackupHints:  []pc.GetHintsResponse_ResponseHints{{}, {}},
	}, resp)
	// Picking hints passes through the supplied shard recovery log.
	require.Equal(t, recoverylog.FSMHints{Log: "a/log"}, PickFirstHints(resp, "a/log"))

	// Now allocate the shard, and then install hint fixtures.
	tf.allocateShard(spec, localID)
	expectStatusCode(t, tf.state, pc.ReplicaStatus_PRIMARY)

	var shard = tf.resolver.shards[shardA]
	var mkHints = func(id int64) recoverylog.FSMHints {
		return recoverylog.FSMHints{
			Log: spec.RecoveryLog(),
			LiveNodes: []recoverylog.FnodeSegments{{
				Fnode: recoverylog.Fnode(id),
				Segments: []recoverylog.Segment{
					{Author: 0x1234, FirstSeqNo: id, LastSeqNo: id},
				},
			}},
		}
	}

	var expected []pc.GetHintsResponse_ResponseHints
	for _, i := range []int64{111, 333, 222} {
		var hints = mkHints(i)
		expected = append(expected, pc.GetHintsResponse_ResponseHints{
			Hints: &hints,
		})
	}
	require.NoError(t, storeRecordedHints(shard, mkHints(111)))
	require.NoError(t, storeRecoveredHints(shard, mkHints(222)))
	require.NoError(t, storeRecoveredHints(shard, mkHints(333)))

	// Case: Correctly fetch hints
	resp, err = tf.service.GetHints(shard.ctx, allClaims, &pc.GetHintsRequest{Shard: shardA})
	require.NoError(t, err)
	require.Equal(t, &pc.GetHintsResponse{
		Status:       pc.Status_OK,
		Header:       resp.Header,
		PrimaryHints: expected[0],
		BackupHints:  expected[1:],
	}, resp)
	require.Equal(t, *expected[0].Hints, PickFirstHints(resp, "a/log"))

	// Case: No primary hints
	_, _ = tf.etcd.Delete(shard.ctx, spec.HintPrimaryKey())
	resp, err = tf.service.GetHints(shard.ctx, allClaims, &pc.GetHintsRequest{Shard: shardA})
	require.NoError(t, err)
	require.Equal(t, &pc.GetHintsResponse{
		Status:       pc.Status_OK,
		Header:       resp.Header,
		PrimaryHints: pc.GetHintsResponse_ResponseHints{},
		BackupHints:  expected[1:],
	}, resp)
	require.Equal(t, *expected[1].Hints, PickFirstHints(resp, "a/log"))

	// Case: Hint key has not yet been written to
	require.NoError(t, storeRecordedHints(shard, mkHints(111)))
	{
		tf.ks.Mu.RLock()
		tf.resolver.shards[shardA].resolved.spec.HintBackups = tf.resolver.shards[shardA].resolved.spec.HintBackups + 1
		tf.ks.Mu.RUnlock()
	}
	resp, err = tf.service.GetHints(shard.ctx, allClaims, &pc.GetHintsRequest{Shard: shardA})
	require.NoError(t, err)
	require.Equal(t, &pc.GetHintsResponse{
		Status:       pc.Status_OK,
		Header:       resp.Header,
		PrimaryHints: expected[0],
		BackupHints:  append(expected[1:], pc.GetHintsResponse_ResponseHints{}),
	}, resp)

	// Case: No backup hints
	{
		tf.ks.Mu.RLock()
		tf.resolver.shards[shardA].resolved.spec.HintBackups = 0
		tf.ks.Mu.RUnlock()
	}
	resp, err = tf.service.GetHints(shard.ctx, allClaims, &pc.GetHintsRequest{Shard: shardA})
	require.NoError(t, err)
	require.Equal(t, &pc.GetHintsResponse{
		Status:       pc.Status_OK,
		Header:       resp.Header,
		PrimaryHints: expected[0],
	}, resp)

	// Case: Fetch hints for a non-existent shard
	resp, err = tf.service.GetHints(shard.ctx, allClaims, &pc.GetHintsRequest{Shard: "missing-shard"})
	require.NoError(t, err)
	require.Equal(t, pc.Status_SHARD_NOT_FOUND, resp.Status)

	// Case: Hint does not correspond to correct recovery log
	var hints = mkHints(444)
	hints.Log = "incorrect/log"
	require.NoError(t, storeRecordedHints(shard, hints))
	resp, err = tf.service.GetHints(shard.ctx, allClaims, &pc.GetHintsRequest{Shard: shardA})
	require.Nil(t, resp)
	require.EqualError(t, err, "hints.Log incorrect/log != ShardSpec.RecoveryLog recovery/logs/shard-A")

	// Case: Invalid hint has been stored
	hints = mkHints(555)
	hints.Log = ""
	require.NoError(t, storeRecordedHints(shard, hints))
	_, err = tf.service.GetHints(shard.ctx, allClaims, &pc.GetHintsRequest{Shard: shardA})
	require.EqualError(t, err, "validating FSMHints: hinted log not provided")

	// Case: Insufficient claimed selector.
	resp, err = tf.service.GetHints(shard.ctx, noClaims, &pc.GetHintsRequest{Shard: shardA})
	require.NoError(t, err)
	require.Equal(t, pc.Status_SHARD_NOT_FOUND, resp.Status)

	tf.allocateShard(spec) // Cleanup.
}

func TestVerifyReferencedJournalsCases(t *testing.T) {
	var tf, cleanup = newTestFixture(t)
	var ctx, jc = context.Background(), tf.broker.Client()
	defer cleanup()

	var req = &pc.ApplyRequest{
		Changes: []pc.ApplyRequest_Change{
			{Upsert: makeShard(shardA)},
			{Delete: "other-shard"},
			{Upsert: makeShard(shardB)},
		},
	}
	// Case: valid request => no error.
	require.NoError(t, VerifyReferencedJournals(ctx, jc, req))

	// Case: shards may omit recovery logs => no error.
	var tmp = req.Changes[0].Upsert.RecoveryLogPrefix
	req.Changes[0].Upsert.RecoveryLogPrefix = ""
	require.NoError(t, VerifyReferencedJournals(ctx, jc, req))
	req.Changes[0].Upsert.RecoveryLogPrefix = tmp

	// Case: recovery log has wrong content type.
	brokertest.CreateJournals(t, tf.broker, brokertest.Journal(
		pb.JournalSpec{Name: pb.Journal(aRecoveryLogPrefix + "/missing-type")}))

	req.Changes[2].Upsert.Id = "missing-type"
	require.EqualError(t, VerifyReferencedJournals(ctx, jc, req),
		"Shard[missing-type]: expected recovery/logs/missing-type to have content-type application/x-gazette-recoverylog but was ''")

	// Case: recovery log doesn't exist.
	req.Changes[2].Upsert.Id = "missing-log"
	require.EqualError(t, VerifyReferencedJournals(ctx, jc, req),
		"Shard[missing-log]: named journal does not exist (recovery/logs/missing-log)")

	req.Changes[2].Upsert.Id = shardB

	// Case: source journal has wrong content type.
	req.Changes[2].Upsert.Sources[1].Journal = pb.Journal(aRecoveryLogPrefix + "/" + shardC)
	require.EqualError(t, VerifyReferencedJournals(ctx, jc, req),
		"Shard[shard-B].Sources[recovery/logs/shard-C] message framing: unrecognized content-type (application/x-gazette-recoverylog)")

	// Case: source journal doesn't exist.
	req.Changes[2].Upsert.Sources[1].Journal = "does/not/exist"
	require.EqualError(t, VerifyReferencedJournals(ctx, jc, req),
		"Shard[shard-B]: named journal does not exist (does/not/exist)")
}

func TestAPIUnassignCases(t *testing.T) {
	var tf, cleanup = newTestFixture(t)
	defer cleanup()
	var restoreTransitions = disableShardTransitions()
	defer restoreTransitions()

	var specA = makeShard(shardA)
	var specB = makeShard(shardB)
	var specC = makeShard(shardC)

	tf.allocateShard(specA)
	tf.allocateShard(specB, remoteID)
	tf.setReplicaStatus(specB, remoteID, 0, pc.ReplicaStatus_PRIMARY)
	tf.allocateShard(specC, localID, remoteID)
	tf.setReplicaStatus(specC, localID, 0, pc.ReplicaStatus_PRIMARY)
	tf.setReplicaStatus(specC, remoteID, 1, pc.ReplicaStatus_FAILED)
	tf.setReplicaStatus(specC, remoteID, 2, pc.ReplicaStatus_STANDBY)
	expectStatusCode(t, tf.state, pc.ReplicaStatus_PRIMARY)

	// Asserts that we have modified the assignment as we expected.
	var check = func(resp *pc.UnassignResponse, expectedSpec *pc.ShardSpec, affectedShards []pc.ShardID, expectedStatuses []pc.ReplicaStatus) {
		require.Equal(t, pc.Status_OK, resp.Status)
		require.Equal(t, affectedShards, resp.Shards)
		// Immediately query for remaining shard replicas.
		listResp, err := tf.service.List(context.Background(), allClaims,
			&pc.ListRequest{
				Selector: pb.LabelSelector{Include: pb.MustLabelSet("id", expectedSpec.Id.String())},
			})
		require.NoError(t, err)
		require.Equal(t, 1, len(listResp.Shards))
		if len(expectedStatuses) > 0 {
			require.Equal(t, 1, len(listResp.Shards[0].Status))
			require.Equal(t, expectedStatuses, []pc.ReplicaStatus{listResp.Shards[0].Status[0]})
		}
	}

	// Case: A shard with no assignments
	resp, err := tf.service.Unassign(context.Background(), allClaims,
		&pc.UnassignRequest{Shards: []pc.ShardID{specA.Id}})
	require.NoError(t, err)
	check(resp, specA, []pc.ShardID{}, []pc.ReplicaStatus{})

	// Case: A shard with a single PRIMARY assignment
	resp, err = tf.service.Unassign(context.Background(), allClaims,
		&pc.UnassignRequest{Shards: []pc.ShardID{specB.Id}})
	require.NoError(t, err)
	check(resp, specB, []pc.ShardID{specB.Id}, []pc.ReplicaStatus{})

	// Case: A shard with multiple assignments. PRIMARY is removed, FAILED is removed, STANDBY remains.
	resp, err = tf.service.Unassign(context.Background(), allClaims,
		&pc.UnassignRequest{Shards: []pc.ShardID{specC.Id}})
	require.NoError(t, err)
	check(resp, specC, []pc.ShardID{specC.Id}, []pc.ReplicaStatus{{Code: pc.ReplicaStatus_STANDBY}})

	// Case: Only unassign failed shards
	tf.allocateShard(specA, localID)
	tf.setReplicaStatus(specA, localID, 0, pc.ReplicaStatus_PRIMARY)
	resp, err = tf.service.Unassign(context.Background(), allClaims,
		&pc.UnassignRequest{Shards: []pc.ShardID{specA.Id}, OnlyFailed: true})
	require.NoError(t, err)
	check(resp, specA, []pc.ShardID{}, []pc.ReplicaStatus{{Code: pc.ReplicaStatus_PRIMARY}})

	tf.setReplicaStatus(specA, localID, 0, pc.ReplicaStatus_FAILED)
	resp, err = tf.service.Unassign(context.Background(), allClaims,
		&pc.UnassignRequest{Shards: []pc.ShardID{specA.Id}, OnlyFailed: true})
	require.NoError(t, err)
	check(resp, specA, []pc.ShardID{specA.Id}, []pc.ReplicaStatus{})

	// Case: Remove multiple unrelated shard assignments at once
	tf.allocateShard(specA, localID)
	tf.setReplicaStatus(specA, localID, 0, pc.ReplicaStatus_FAILED)
	tf.allocateShard(specB, localID)
	tf.setReplicaStatus(specB, localID, 0, pc.ReplicaStatus_FAILED)
	tf.allocateShard(specC, localID)
	tf.setReplicaStatus(specC, localID, 0, pc.ReplicaStatus_PRIMARY)
	resp, err = tf.service.Unassign(context.Background(), allClaims,
		&pc.UnassignRequest{Shards: []pc.ShardID{specA.Id, specB.Id, specC.Id}, OnlyFailed: true})
	require.NoError(t, err)
	check(resp, specA, []pc.ShardID{specA.Id, specB.Id}, []pc.ReplicaStatus{})
	check(resp, specB, []pc.ShardID{specA.Id, specB.Id}, []pc.ReplicaStatus{})
	check(resp, specC, []pc.ShardID{specA.Id, specB.Id}, []pc.ReplicaStatus{{Code: pc.ReplicaStatus_PRIMARY}})

	// Case: Insufficient claimed capability
	_, err = tf.service.Unassign(context.Background(), noClaims,
		&pc.UnassignRequest{Shards: []pc.ShardID{specB.Id}})
	require.EqualError(t, err, `rpc error: code = Unauthenticated desc = not authorized to shard-B`)

	// Cleanup.
	tf.allocateShard(specA)
	tf.allocateShard(specB)
	tf.allocateShard(specC)
}

var (
	allClaims = pb.Claims{
		Capability: pb.Capability_ALL,
	}
	noClaims = pb.Claims{
		Capability: pb.Capability_APPLY,
		Selector:   pb.MustLabelSelector("this-does=not-match"),
	}
)
