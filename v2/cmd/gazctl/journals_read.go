package main

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"

	"github.com/LiveRamp/gazette/v2/pkg/client"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
)

type cmdJournalRead struct {
	ReadConfig
}

func (cmd *cmdJournalRead) Execute([]string) error {
	startup()

	var err error
	var ctx = context.Background()
	var brokerClient = journalsCfg.Broker.RoutedJournalClient(ctx)

	// Get the list of journals which match this selector.
	var listRequest pb.ListRequest
	listRequest.Selector, err = pb.ParseLabelSelector(cmd.Selector)
	mbp.Must(err, "failed to parse label selector", "selector", cmd.Selector)

	var listResp *pb.ListResponse
	listResp, err = client.ListAll(ctx, brokerClient, listRequest)
	mbp.Must(err, "failed to resolved journals from selector", cmd.Selector)

	var doneCounter int32
	var dataChan = make(chan []byte)
	var errChan = make(chan error)
	for _, journal := range listResp.Journals {
		go createJournalReader(createJournalReaderOpts{
			dataChan: dataChan,
			errChan:  errChan,
			ctx:      ctx,
			spec:     journal.Spec,
			client:   brokerClient,
			blocking: cmd.Blocking,
			offset:   cmd.Offset,
		})
		if !cmd.Blocking {
			doneCounter++
		}
	}

	for {
		select {
		case data := <-dataChan:
			var _, err = os.Stdout.Write(data)
			if err != nil {
				mbp.Must(err, "failed to write data")
				return nil
			}
		case err := <-errChan:
			// This is the error that is returned a journal has been read to head.
			// If blocking has been set wait for all values to readers to reach head before exiting.
			if err == client.ErrOffsetNotYetAvailable && !cmd.Blocking {
				doneCounter--
				if doneCounter == 0 {
					return nil
				}
				continue
			}
			mbp.Must(err, "error reading from journal")
			return nil
		}
	}
}

type createJournalReaderOpts struct {
	dataChan chan<- []byte
	errChan  chan<- error
	ctx      context.Context
	spec     pb.JournalSpec
	client   pb.RoutedJournalClient
	blocking bool
	offset   int64
}

func createJournalReader(opts createJournalReaderOpts) error {
	var req pb.ReadRequest
	req.Journal = opts.spec.Name
	req.Offset = opts.offset
	req.Block = opts.blocking

	var currentFragmentEnd int64
	var writeHead int64
	var reader = client.NewReader(opts.ctx, opts.client, req)

	var fragmentBuffer = bytes.NewBuffer([]byte{})
	// default size of a buffered reader. Is there a better size?
	var content = make([]byte, 4096)
	for {
		var bytesWritten, err = reader.Read(content)
		// Offset jumps means that the offset is set to -1, or data has been removed from
		// a journal. In either case this error is expected and should not be surfaced.
		if (err != nil) && (err != client.ErrOffsetJump) {
			opts.errChan <- err
			return nil
		}

		// The response contains metadata information about the current fragment.
		// Set the local writeHead to match up with the beginning of the fragment.
		if reader.Response.Fragment != nil {
			currentFragmentEnd = reader.Response.Fragment.End
			writeHead = reader.Request.Offset
			continue

		}

		if bytesWritten > 0 {
			_, err := fragmentBuffer.Write(content[:bytesWritten])
			if err != nil {
				opts.errChan <- err
				return nil
			}
			writeHead += int64(bytesWritten)
		}
		// When a fragment end has been reached we can be sure we have full
		// messages and the fragmentBuffer can be flushed.k
		if writeHead == currentFragmentEnd {
			var fragmentBytes, err = ioutil.ReadAll(fragmentBuffer)
			if err != nil {
				opts.errChan <- err
				return nil
			}
			opts.dataChan <- fragmentBytes
		}
	}
}
