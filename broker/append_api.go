package broker

import (
	"context"
	"io"
	"net"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// Append dispatches the JournalServer.Append API.
func (svc *Service) Append(claims pb.Claims, stream pb.Journal_AppendServer) (err error) {
	var (
		fsm appendFSM
		req *pb.AppendRequest
	)
	defer instrumentJournalServerRPC("Append", &err, &fsm.resolved)()

	defer func() {
		if err != nil {
			var addr net.Addr
			if p, ok := peer.FromContext(stream.Context()); ok {
				addr = p.Addr
			}
			log.WithFields(log.Fields{"err": err, "req": req, "client": addr}).
				Warn("served Append RPC failed")
		}
	}()

	if req, err = stream.Recv(); err != nil {
		return err
	} else if err = req.Validate(); err != nil {
		return err
	}

	fsm = appendFSM{
		svc:    svc,
		ctx:    stream.Context(),
		claims: claims,
		req:    *req,
	}
	fsm.run(stream.Recv)

	switch fsm.state {
	case stateProxy:
		req.Header = &fsm.resolved.Header // Attach resolved Header to |req|, which we'll forward.
		return proxyAppend(stream, *req, svc.jc)
	case stateFinished:
		writeHeadGauge.WithLabelValues(fsm.clientFragment.Journal.String()).
			Set(float64(fsm.clientFragment.End))

		return stream.SendAndClose(&pb.AppendResponse{
			Status:        pb.Status_OK,
			Header:        fsm.resolved.Header,
			Commit:        fsm.clientFragment,
			Registers:     &fsm.registers,
			TotalChunks:   fsm.clientTotalChunks,
			DelayedChunks: fsm.clientDelayedChunks,
		})
	case stateError:
		if fsm.resolved.status != pb.Status_OK {
			var resp = &pb.AppendResponse{
				Status: fsm.resolved.status,
				Header: fsm.resolved.Header,
			}
			if fsm.resolved.status == pb.Status_REGISTER_MISMATCH {
				resp.Registers = &fsm.registers
			}
			if fsm.resolved.status == pb.Status_FRAGMENT_STORE_UNHEALTHY {
				resp.StoreHealthError = fsm.err.Error()
			}
			return stream.SendAndClose(resp)
		}
		// Client-initiated RPC cancellations are expected errors.
		if errors.Cause(fsm.err) == context.Canceled {
			return nil
		}
		return fsm.err
	default:
		panic("not reached")
	}
}

// proxyAppend forwards an AppendRequest to a resolved peer broker.
// Pass request by value as we'll later mutate it (via RecvMsg).
func proxyAppend(stream grpc.ServerStream, req pb.AppendRequest, jc pb.JournalClient) error {
	// We verified the client's authorization & claims and are running under its context.
	// pb.AuthJournalClient will self-sign claims to proxy this journal on the client's behalf.
	var ctx = pb.WithClaims(stream.Context(), pb.Claims{
		Capability: pb.Capability_APPEND,
		Selector: pb.LabelSelector{
			Include: pb.MustLabelSet("name", req.Journal.String()),
		},
	})
	ctx = pb.WithDispatchRoute(ctx, req.Header.Route, req.Header.ProcessId)

	var client, err = jc.Append(ctx)
	if err != nil {
		return err
	}
	for {
		if err = client.SendMsg(&req); err != nil {
			break // Client stream is broken. RecvMsg() will return causal error.
		} else if err = stream.RecvMsg(&req); err == io.EOF {
			_ = client.CloseSend()
			break
		} else if err != nil {
			_, _ = client.CloseAndRecv() // Drain to free resources.
			return err
		}
	}

	// We don't use CloseAndRecv() here because it returns a confusing
	// EOF error if the stream was broken by the peer (from it's own CloseSend()
	// call under the hood), rather than the actually informative RecvMsg error.
	var resp = new(pb.AppendResponse)
	if err = client.RecvMsg(resp); err != nil {
		return err
	} else {
		// Extra RecvMsg to explicitly read EOF, as a work-around for
		// https://github.com/grpc-ecosystem/go-grpc-prometheus/issues/92
		_ = client.RecvMsg(new(pb.AppendResponse))

		return stream.SendMsg(resp)
	}
}
