package broker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/etcdtest"
)

func TestListCases(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	// Create a fixture of JournalSpecs which we'll list.
	var fragSpec = pb.JournalSpec_Fragment{
		Length:           1024,
		RefreshInterval:  time.Second,
		CompressionCodec: pb.CompressionCodec_SNAPPY,
	}
	var specA = &pb.JournalSpec{
		Name:        "journal/1/A",
		LabelSet:    pb.MustLabelSet("foo", "bar"),
		Replication: 1,
		Fragment:    fragSpec,
	}
	var specB = &pb.JournalSpec{
		Name:        "journal/2/B",
		LabelSet:    pb.MustLabelSet("bar", "baz"),
		Replication: 1,
		Fragment:    fragSpec,
	}
	var specC = &pb.JournalSpec{
		Name:        "journal/1/C",
		Replication: 1,
		Fragment:    fragSpec,
	}

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var fixtureRevision int64
	{
		var resp, err = broker.client().Apply(ctx, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{Upsert: specA},
				{Upsert: specB},
				{Upsert: specC},
			},
		})
		require.NoError(t, err)
		require.Equal(t, pb.Status_OK, resp.Status)
		fixtureRevision = resp.Header.Etcd.Revision
	}

	// Assign |specC| to |bk|, to verify its returned non-empty Route.
	var rev = mustKeyValues(t, etcd, map[string]string{
		allocator.AssignmentKey(broker.ks, allocator.Assignment{
			ItemID:       "journal/1/C",
			MemberZone:   "local",
			MemberSuffix: "broker",
			Slot:         1,
		}): ""})

	broker.ks.Mu.RLock()
	require.NoError(t, broker.ks.WaitForRevision(ctx, rev))
	broker.ks.Mu.RUnlock()

	var verify = func(resp *pb.ListResponse, expect ...*pb.JournalSpec) {
		require.Equal(t, pb.Status_OK, resp.Status)
		require.Len(t, resp.Journals, len(expect))

		for i, exp := range expect {
			require.Equal(t, *exp, resp.Journals[i].Spec)

			require.Equal(t, fixtureRevision, resp.Journals[i].CreateRevision)
			require.Equal(t, fixtureRevision, resp.Journals[i].ModRevision)

			if exp == specC {
				require.Equal(t, []pb.ProcessSpec_ID{{Zone: "local", Suffix: "broker"}},
					resp.Journals[i].Route.Members)
				require.Equal(t, []pb.Endpoint{broker.srv.Endpoint()},
					resp.Journals[i].Route.Endpoints)
			} else {
				require.Nil(t, resp.Journals[i].Route.Members)
				require.Nil(t, resp.Journals[i].Route.Endpoints)
			}
		}
	}

	// Case: Empty selector returns all shards.
	var resp, err = broker.client().List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{},
	})
	require.NoError(t, err)
	verify(resp, specA, specC, specB)

	// Case: Exclude on label.
	resp, err = broker.client().List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{Exclude: pb.MustLabelSet("foo", "")},
	})
	require.NoError(t, err)
	verify(resp, specC, specB)

	// Case: Meta-label "name" selects journals by name.
	resp, err = broker.client().List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("name", "journal/2/B")},
	})
	require.NoError(t, err)
	verify(resp, specB)

	// Case: Meta-label "prefix" selects journals by name prefix.
	resp, err = broker.client().List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("prefix", "journal/1/")},
	})
	require.NoError(t, err)
	verify(resp, specA, specC)

	// Case: Errors on request validation error.
	_, err = broker.client().List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("prefix", "invalid/because/missing/trailing/slash")},
	})
	require.Regexp(t, `.* Selector.Include.Labels\["prefix"\]: expected trailing '/' (.*)`, err)

	broker.cleanup()
}

func TestApplyCases(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	// Fixtures for use in the test.
	var fragSpec = pb.JournalSpec_Fragment{
		Length:           1024,
		RefreshInterval:  time.Second,
		CompressionCodec: pb.CompressionCodec_SNAPPY,
	}
	var specA = pb.JournalSpec{
		Name:        "journal/A",
		Replication: 1,
		Fragment:    fragSpec,
	}
	var specB = pb.JournalSpec{
		Name:        "journal/B",
		Replication: 1,
		Fragment:    fragSpec,
	}
	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})

	var verifyAndFetchRev = func(name pb.Journal, expect pb.JournalSpec) int64 {
		var resp, err = broker.client().List(ctx, &pb.ListRequest{
			Selector: pb.LabelSelector{Include: pb.MustLabelSet("name", name.String())},
		})
		require.NoError(t, err)
		require.Equal(t, pb.Status_OK, resp.Status)
		require.Equal(t, expect, resp.Journals[0].Spec)
		return resp.Journals[0].ModRevision
	}

	var must = func(r *pb.ApplyResponse, err error) *pb.ApplyResponse {
		require.NoError(t, err)
		return r
	}

	// Case: Create new specs A & B.
	require.Equal(t, pb.Status_OK,
		must(broker.client().Apply(ctx, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{Upsert: &specA},
				{Upsert: &specB},
			},
		})).Status)

	// Case: Update existing spec B.
	var origSpecB = specB
	specB.Labels = append(specB.Labels, pb.Label{Name: "foo", Value: "bar"})

	require.Equal(t, pb.Status_OK,
		must(broker.client().Apply(ctx, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{Upsert: &specB, ExpectModRevision: verifyAndFetchRev("journal/B", origSpecB)},
			},
		})).Status)

	// Case: Delete existing spec A.
	require.Equal(t, pb.Status_OK,
		must(broker.client().Apply(ctx, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{Delete: "journal/A", ExpectModRevision: verifyAndFetchRev("journal/A", specA)},
			},
		})).Status)

	// Case: Deletion at wrong revision fails.
	require.Equal(t, pb.Status_ETCD_TRANSACTION_FAILED,
		must(broker.client().Apply(ctx, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{Delete: "journal/B", ExpectModRevision: verifyAndFetchRev("journal/B", specB) - 1},
			},
		})).Status)

	// Case: Update at wrong revision fails.
	require.Equal(t, pb.Status_ETCD_TRANSACTION_FAILED,
		must(broker.client().Apply(ctx, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{Upsert: &specB, ExpectModRevision: verifyAndFetchRev("journal/B", specB) - 1},
			},
		})).Status)

	// Case: Update with explicit revision of -1 succeeds.
	require.Equal(t, pb.Status_OK,
		must(broker.client().Apply(ctx, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{Upsert: &specB, ExpectModRevision: -1},
			},
		})).Status)

	// Case: Deletion with explicit revision of -1 succeeds.
	require.Equal(t, pb.Status_OK,
		must(broker.client().Apply(ctx, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{Delete: "journal/B", ExpectModRevision: -1},
			},
		})).Status)

	// Case: Invalid requests fail with an error.
	var _, err = broker.client().Apply(ctx, &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{{Delete: "invalid journal name"}},
	})
	require.Regexp(t, `.* Changes\[0\].Delete: not a valid token \(invalid journal name\)`, err)

	broker.cleanup()
}
