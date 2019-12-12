package broker

import (
	"context"
	"net"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc/peer"
)

// List dispatches the JournalServer.List API.
func (svc *Service) List(ctx context.Context, req *pb.ListRequest) (resp *pb.ListResponse, err error) {
	defer instrumentJournalServerOp("List", &err, nil, time.Now())

	defer func() {
		if err != nil {
			var addr net.Addr
			if p, ok := peer.FromContext(ctx); ok {
				addr = p.Addr
			}
			log.WithFields(log.Fields{"err": err, "req": req, "client": addr}).
				Warn("served List RPC failed")
		}
	}()

	var s = svc.resolver.state

	resp = &pb.ListResponse{
		Status: pb.Status_OK,
		Header: pb.NewUnroutedHeader(s),
	}
	if err = req.Validate(); err != nil {
		return resp, err
	}

	// TODO(johnny): Implement support for PageLimit & PageToken.

	var metaLabels, allLabels pb.LabelSet

	defer s.KS.Mu.RUnlock()
	s.KS.Mu.RLock()

	var it = allocator.LeftJoin{
		LenL: len(s.Items),
		LenR: len(s.Assignments),
		Compare: func(l, r int) int {
			var lID = s.Items[l].Decoded.(allocator.Item).ID
			var rID = s.Assignments[r].Decoded.(allocator.Assignment).ItemID
			return strings.Compare(lID, rID)
		},
	}
	for cur, ok := it.Next(); ok; cur, ok = it.Next() {
		var journal = pb.ListResponse_Journal{
			Spec: *s.Items[cur.Left].Decoded.(allocator.Item).ItemValue.(*pb.JournalSpec)}

		metaLabels = pb.ExtractJournalSpecMetaLabels(&journal.Spec, metaLabels)
		allLabels = pb.UnionLabelSets(metaLabels, journal.Spec.LabelSet, allLabels)

		if !req.Selector.Matches(allLabels) {
			continue
		}
		journal.ModRevision = s.Items[cur.Left].Raw.ModRevision
		journal.Route.Init(s.Assignments[cur.RightBegin:cur.RightEnd])
		journal.Route.AttachEndpoints(s.KS)

		resp.Journals = append(resp.Journals, journal)
	}
	return resp, nil
}

// Apply dispatches the JournalServer.Apply API.
func (svc *Service) Apply(ctx context.Context, req *pb.ApplyRequest) (resp *pb.ApplyResponse, err error) {
	defer instrumentJournalServerOp("Apply", &err, nil, time.Now())

	defer func() {
		if err != nil {
			var addr net.Addr
			if p, ok := peer.FromContext(ctx); ok {
				addr = p.Addr
			}
			log.WithFields(log.Fields{"err": err, "req": req, "client": addr}).
				Warn("served Apply RPC failed")
		}
	}()

	var s = svc.resolver.state

	resp = &pb.ApplyResponse{
		Status: pb.Status_OK,
		Header: pb.NewUnroutedHeader(s),
	}
	if err = req.Validate(); err != nil {
		return resp, err
	}

	var cmp []clientv3.Cmp
	var ops []clientv3.Op

	for _, change := range req.Changes {
		var key string

		if change.Upsert != nil {
			key = allocator.ItemKey(s.KS, change.Upsert.Name.String())
			ops = append(ops, clientv3.OpPut(key, change.Upsert.MarshalString()))
		} else {
			key = allocator.ItemKey(s.KS, change.Delete.String())
			ops = append(ops, clientv3.OpDelete(key))
		}

		// Allow caller to explicitly ignore revision comparison
		// by passing a value of -1 for revision.
		if change.ExpectModRevision != -1 {
			cmp = append(cmp, clientv3.Compare(clientv3.ModRevision(key), "=", change.ExpectModRevision))
		}
	}

	var txnResp clientv3.OpResponse
	if txnResp, err = svc.etcd.Do(ctx, clientv3.OpTxn(cmp, ops, nil)); err != nil {
		return resp, err
	} else if !txnResp.Txn().Succeeded {
		resp.Status = pb.Status_ETCD_TRANSACTION_FAILED
	} else {
		// Delay responding until we have read our own Etcd write.
		s.KS.Mu.RLock()
		err = s.KS.WaitForRevision(ctx, txnResp.Txn().Header.Revision)
		s.KS.Mu.RUnlock()
	}
	return resp, err
}
