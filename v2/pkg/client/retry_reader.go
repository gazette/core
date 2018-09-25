package client

import (
	"bufio"
	"context"
	"errors"
	"io"
	"time"

	log "github.com/sirupsen/logrus"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// RetryReader wraps Reader with error handling and retry behavior, as well as
// support for cancellation of an ongoing Read or Seek operation. RetryReader
// is not thread-safe, with one exception: Cancel may be called from one
// goroutine to abort an ongoing Read or Seek call in another.
type RetryReader struct {
	// Reader is the current underlying Reader of the RetryReader. This instance
	// may change many times over the lifetime of a RetryReader, as Read RPCs
	// finish or are cancelled and then restarted.
	Reader *Reader
	// Cancel Read operations of the current Reader. Notably this will cause an
	// ongoing blocked Read (as well as any future Reads) to return a "Cancelled"
	// error. Restart may be called to re-initialize the RetryReader.
	Cancel context.CancelFunc

	ctx    context.Context
	client ReaderClient
}

// NewRetryReader returns a RetryReader initialized with the BrokerClient and ReadRequest.
func NewRetryReader(ctx context.Context, client ReaderClient, req pb.ReadRequest) *RetryReader {
	// If our BrokerClient is capable of directly routing to responsible brokers,
	// we don't want brokers to proxy to peers or remote Fragments on our behalf.
	if _, ok := client.(RouteUpdater); ok {
		req.DoNotProxy = true
	}

	var rr = &RetryReader{
		ctx:    ctx,
		client: client,
	}
	rr.Restart(req)
	return rr
}

// Journal being read by this RetryReader.
func (rr *RetryReader) Journal() pb.Journal {
	return rr.Reader.Request.Journal
}

// Offset of the next Journal byte to be returned by Read.
func (rr *RetryReader) Offset() int64 {
	return rr.Reader.Request.Offset
}

// Read returns the next bytes of journal content. It will return a non-nil
// error in the following cases:
//  * Cancel is called, or the RetryReader context is cancelled.
//  * The broker returns OFFSET_NOT_YET_AVAILABLE (ErrOffsetNotYetAvailable)
//    for a non-blocking ReadRequest.
//  * An offset jump occurred (ErrOffsetJump), in which case the client
//    should inspect the new Offset may continue reading if desired.
// All other errors are retried.
func (rr *RetryReader) Read(p []byte) (n int, err error) {
	for i := 0; true; i++ {

		if n, err = rr.Reader.Read(p); err == nil {
			return // Success.
		} else if err == ErrOffsetJump {
			return // Note |rr.Reader| is not invalidated by this error.
		}

		// Our Read failed. Since we're a retrying reader, we consume and mask
		// errors (possibly logging a warning), manage our own back-off timer,
		// and restart the stream when ready for another attempt.

		// Context cancellations are usually wrapped by augmenting errors as they
		// return up the call stack. If our Reader's context is Done, assume
		// that is the primary error.
		if rr.Reader.ctx.Err() != nil {
			err = rr.Reader.ctx.Err()
		}

		// Restart the Reader re-using the same context (note we could be racing
		// this restart with a concurrent call to |rr.Cancel|).
		rr.Reader = NewReader(rr.Reader.ctx, rr.Reader.client, rr.Reader.Request)

		switch err {
		case context.DeadlineExceeded, context.Canceled:
			return
		case ErrOffsetNotYetAvailable:
			if rr.Reader.Request.Block {
				// |Block| was set after a non-blocking reader was started. Restart in blocking mode.
			} else {
				return // Surface to caller.
			}
		case io.EOF, ErrNotJournalBroker:
			// Suppress logging for expected errors.
		default:
			log.WithFields(log.Fields{"journal": rr.Journal(), "offset": rr.Offset(), "err": err}).
				Warn("read failure (will retry)")
		}

		// Wait for a back-off timer, or context cancellation.
		select {
		case <-rr.Reader.ctx.Done():
			return 0, rr.Reader.ctx.Err()
		case <-time.After(backoff(i)):
		}
	}
	panic("not reached")
}

// Seek sets the offset for the next Read. It returns an error if (and only if)
// |whence| is io.SeekEnd, which is not supported.
func (rr *RetryReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		// |offset| is already absolute.
	case io.SeekCurrent:
		offset = rr.Reader.Request.Offset + offset
	case io.SeekEnd:
		return rr.Reader.Request.Offset, errors.New("io.SeekEnd whence is not supported")
	default:
		panic("invalid whence")
	}

	if _, err := rr.Reader.Seek(offset, io.SeekStart); err != nil {
		if err != ErrSeekRequiresNewReader {
			log.WithFields(log.Fields{"journal": rr.Journal(), "offset": offset, "err": err}).
				Warn("failed to seek open Reader (will retry)")
		}

		var req = rr.Reader.Request
		req.Offset = offset

		rr.Cancel()
		rr.Restart(req)
	}
	return rr.Reader.Request.Offset, nil
}

// AdjustedOffset returns the current journal offset, adjusted for content read
// by |br| (which wraps this RetryReader) but not yet consumed from |br|'s buffer.
func (rr *RetryReader) AdjustedOffset(br *bufio.Reader) int64 { return rr.Reader.AdjustedOffset(br) }

// AdjustedSeek sets the offset for the next Read, accounting for buffered data and updating
// the buffer as needed.
func (rr *RetryReader) AdjustedSeek(offset int64, whence int, br *bufio.Reader) (int64, error) {
	switch whence {
	case io.SeekStart:
		// |offset| is already absolute.
	case io.SeekCurrent:
		offset = rr.AdjustedOffset(br) + offset
	case io.SeekEnd:
		return rr.AdjustedOffset(br), errors.New("io.SeekEnd whence is not supported")
	default:
		panic("invalid whence")
	}

	var delta = offset - rr.AdjustedOffset(br)

	// Fast path: can we fulfill the seek by discarding a portion of buffered data?
	if delta >= 0 && delta <= int64(br.Buffered()) {
		br.Discard(int(delta))
		return offset, nil
	}

	// We must Seek the underlying reader, discarding and resetting the current buffer.
	var n, err = rr.Seek(offset, io.SeekStart)
	br.Reset(rr)
	return n, err
}

// Restart the RetryReader with a new ReadRequest.
func (rr *RetryReader) Restart(req pb.ReadRequest) {
	var ctx, cancel = context.WithCancel(rr.ctx)

	rr.Reader = NewReader(ctx, rr.client, req)
	rr.Cancel = cancel
}

func backoff(attempt int) time.Duration {
	switch attempt {
	case 0, 1:
		return 0
	case 2:
		return time.Millisecond * 5
	case 3, 4, 5:
		return time.Second * time.Duration(attempt-1)
	default:
		return 5 * time.Second
	}
}
