// +build !norocksdb

// Package stream_sum is an example application consisting of three stages:
//
// 1) A `chunker` job randomly generates a number of unique "streams", with
//   stream content emitted across a number of interleaved data chunks.
//
// 2) A `summer` consumer accumulates stream chunks and computes a running
//   SHA1-sum of each stream's content. When the stream is completed,
//   the `summer` consumer emits a final sum to an output journal.
//
// 3) Having written a complete stream, the `chunker` job confirms that the
//   correct sum is written to the output journal.
//
// The `chunker` and `summer` tasks may be independently scaled, and are
// invariant to process failures and restarts.
//
package stream_sum

import (
	"bufio"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc64"
	"io"
	"sync"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	"github.com/LiveRamp/gazette/v2/pkg/mainboilerplate/runconsumer"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/LiveRamp/gazette/v2/pkg/recoverylog"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// ChunkerConfig is the configuration used by the `chunker` job binary.
type ChunkerConfig struct {
	Chunker struct {
		mbp.ZoneConfig
		Streams int `long:"streams" default:"-1" description:"Total number of streams to create. <0 for infinite"`
		Chunks  int `long:"chunks" default:"100" description:"Number of chunks per stream"`
	} `group:"Chunker" namespace:"chunker" env-namespace:"CHUNKER"`

	Broker      mbp.ClientConfig      `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

const (
	// chunksTopicLabel identifies journals which are partitions of chunk messages.
	chunksTopicLabel = "examples/stream-sum/chunks"
	// FinalSumsJournal to which final stream sums are written.
	FinalSumsJournal pb.Journal = "examples/stream-sum/sums"
)

// StreamID uniquely identifies a stream.
type StreamID [16]byte

// Chunk is an ordered slice of stream content.
type Chunk struct {
	ID    StreamID // Unique ID of the stream.
	SeqNo int      // Monotonic sequence number, starting from 1.
	Data  []byte   // Raw data included in the Value. If empty, this is the stream's final chunk.
}

// Sum represents a partial or final CRC64 sum of a stream.
type Sum struct {
	ID    StreamID // Unique ID of the stream.
	SeqNo int      // SeqNo of last Chunk summed over.
	Value uint64   // Computed sum through SeqNo.
}

// Update folds a Chunk into this Sum, returning whether this is the last Chunk of the Stream.
// Update requires that SeqNo be totally ordered, however replays of previous SeqNo are ignored.
func (s *Sum) Update(chunk Chunk) (done bool, err error) {
	if chunk.SeqNo <= s.SeqNo {
		return false, nil // Replay of older message. Ignore.
	} else if chunk.SeqNo > s.SeqNo+1 {
		return true, fmt.Errorf("invalid chunk.SeqNo (%d; sum.SeqNo %d; id %x)",
			chunk.SeqNo, s.SeqNo, chunk.ID)
	} else if chunk.ID != s.ID {
		return true, fmt.Errorf("invalid chunk.ID (%x; sum.ID %x)", chunk.ID, s.ID)
	}

	s.SeqNo = chunk.SeqNo
	s.Value = crc64.Update(s.Value, ecmaTable, chunk.Data)
	return len(chunk.Data) == 0, nil
}

// GenerateAndVerifyStreams is the main routine of the `chunker` job. It
// generates and verifies streams based on the ChunkerConfig.
func GenerateAndVerifyStreams(ctx context.Context, cfg *ChunkerConfig) error {
	var rjc = cfg.Broker.RoutedJournalClient(ctx)
	var as = client.NewAppendService(ctx, rjc)

	var chunksMapping, err = newChunkMapping(ctx, rjc)
	if err != nil {
		return errors.Wrap(err, "building chunks mapping")
	}

	// Issue an empty append transaction (a write barrier) to determine the
	// current write-head of the |FinalSumsJournal|, prior to emitting any chunks.
	var barrier = as.StartAppend(FinalSumsJournal)
	if err = barrier.Release(); err != nil {
		return errors.Wrap(err, "writing barrier")
	}
	<-barrier.Done()

	var rr = client.NewRetryReader(ctx, rjc, pb.ReadRequest{
		Journal:    barrier.Response().Commit.Journal,
		Offset:     barrier.Response().Commit.End,
		Block:      true,
		DoNotProxy: !rjc.IsNoopRouter(),
	})

	var chunkCh = make(chan Chunk)
	var verifyCh = make(chan Sum)
	var actualCh = make(chan Sum)

	go pumpSums(bufio.NewReader(rr), actualCh)
	go generate(cfg.Chunker.Streams, cfg.Chunker.Chunks, verifyCh, chunkCh)

	verify(func(chunk Chunk) {
		var _, err2 = message.Publish(as, chunksMapping, &chunk)
		mbp.Must(err2, "publishing chunk")
	}, chunkCh, verifyCh, actualCh)

	return nil
}

// Summer consumes stream chunks, aggregates chunk data, and emits final sums.
// It implements the runconsumer.Application interface.
type Summer struct{}

// NewConfig returns a new BaseConfig.
func (Summer) NewConfig() runconsumer.Config { return new(struct{ runconsumer.BaseConfig }) }

// InitApplication is a no-op, as Summer provides no client-facing APIs.
func (Summer) InitApplication(args runconsumer.InitArgs) error { return nil }

// NewStore builds a RocksDB store for the Shard. consumer.Application implementation.
func (Summer) NewStore(shard consumer.Shard, dir string, rec *recoverylog.Recorder) (consumer.Store, error) {
	var rdb = consumer.NewRocksDBStore(rec, dir)
	rdb.Cache = make(map[StreamID]Sum)
	return rdb, rdb.Open()
}

// NewMessage returns a Chunk message. consumer.Application implementation.
func (Summer) NewMessage(*pb.JournalSpec) (message.Message, error) { return new(Chunk), nil }

// ConsumeMessage folds a Chunk into its respective partial stream sum.
// If the Chunk represents a stream EOF, it emits a final sum.
// consumer.Application implementation.
func (Summer) ConsumeMessage(shard consumer.Shard, store consumer.Store, env message.Envelope) error {
	var rdb = store.(*consumer.RocksDBStore)
	var cache = rdb.Cache.(map[StreamID]Sum)
	var chunk = env.Message.(*Chunk)

	var sum, ok = cache[chunk.ID]
	if !ok {
		// Fill from database.
		if b, err := rdb.DB.GetBytes(rdb.ReadOptions, chunk.ID[:]); err != nil {
			log.WithFields(log.Fields{"err": err, "id": chunk.ID}).Fatal("reading db")
		} else if len(b) == 0 {
			// Miss. Initialize a new stream.
			sum.ID = chunk.ID
		} else if err = json.Unmarshal(b, &sum); err != nil {
			log.WithFields(log.Fields{"err": err, "id": chunk.ID}).Fatal("unmarshalling record")
		}
	}

	if done, err := sum.Update(*chunk); err != nil {
		log.WithFields(log.Fields{"err": err, "id": chunk.ID}).Fatal("updating record")
	} else if done {
		// Publish completed sum.
		if _, err = message.Publish(shard.JournalClient(), finalSumsMapping, &sum); err != nil {
			log.WithFields(log.Fields{"err": err, "id": sum.ID}).Fatal("publishing sum")
		}
	}

	// Retain the partial sum for the next chunk or flush.
	cache[sum.ID] = sum

	return nil
}

// FinalizeTxn marshals partial stream sums to the |store| to ensure persistence
// across consumer transactions. consumer.Application implementation.
func (Summer) FinalizeTxn(shard consumer.Shard, store consumer.Store) error {
	var rdb = store.(*consumer.RocksDBStore)
	var cache = rdb.Cache.(map[StreamID]Sum)

	for id, sum := range cache {
		delete(cache, id) // Clear for next transaction.

		if b, err := json.Marshal(sum); err != nil {
			log.WithFields(log.Fields{"err": err}).Fatal("marshalling record")
		} else {
			rdb.WriteBatch.Put(id[:], b)
		}
	}
	return nil
}

func generate(numStreams, chunksPerStream int, doneCh chan<- Sum, outCh chan<- Chunk) {
	var streams []Sum

	defer close(outCh)
	defer close(doneCh)

	for {
		// Select an existing stream (if i < len(streams)) or new stream (if i == len(streams)).
		var N = len(streams)

		if numStreams != 0 {
			N += 1
		} else if N == 0 {
			return // Termination condition.
		}
		var i = prngInt(N)

		// Create a new stream.
		if i == len(streams) {
			streams = append(streams, Sum{ID: prngStreamID()})
			numStreams--
		}

		var data []byte
		if streams[i].SeqNo < chunksPerStream {
			data = fillPRNG(make([]byte, 32))
		}

		var chunk = Chunk{
			ID:    streams[i].ID,
			SeqNo: streams[i].SeqNo + 1,
			Data:  data,
		}

		done, err := streams[i].Update(chunk)
		mbp.Must(err, "update failed (invalid SeqNo?)", "ID", chunk.ID)

		outCh <- chunk

		if done {
			doneCh <- streams[i]
			copy(streams[i:], streams[i+1:])
			streams = streams[:len(streams)-1]
		}
	}
}

func verify(emit func(Chunk), chunkCh <-chan Chunk, verifyCh, actualCh <-chan Sum) {
	for chunkCh != nil || verifyCh != nil {
		// Emit chunks as they're generated, and verify as they complete. If a
		// stream is ready for verification, we block and wait for it's computed
		// sum prior to emitting any further chunks. This precludes races were
		// expected and actual stream sums potentially arrive in different orders
		// (since we can't emit a chunk which would complete a next stream before
		// we've verified the last one).

		select {
		case chunk, ok := <-chunkCh:
			if !ok {
				chunkCh = nil
				continue
			}

			emit(chunk)

		case expect, ok := <-verifyCh:
			if !ok {
				verifyCh = nil
				continue
			}

			for actual := range actualCh {
				if actual.ID == expect.ID {
					if actual != expect {
						log.WithFields(log.Fields{"actual": actual, "expect": expect}).Fatal("mis-matched sum!")
					} else {
						log.WithFields(log.Fields{"id": actual.ID}).Info("verified sum")
					}
					break
				}
			}
		}
	}
}

// pumpSums reads Sum messages from the Reader, and dispatches to |ch|.
func pumpSums(br *bufio.Reader, ch chan<- Sum) {
	for {
		var sum Sum

		if b, err := message.JSONFraming.Unpack(br); err == context.Canceled || err == io.EOF {
			return
		} else if err != nil {
			log.WithField("err", err).Fatal("unpacking frame")
		} else if err = message.JSONFraming.Unmarshal(b, &sum); err != nil {
			log.WithField("err", err).Fatal("failed to Unmarshal Sum")
		} else {
			ch <- sum
		}
	}
}

// newChunkMapping returns a MappingFunc over journals identified by |chunkPartitionsLabel|.
func newChunkMapping(ctx context.Context, jc pb.JournalClient) (message.MappingFunc, error) {
	if chunkParts, err := client.NewPolledList(ctx, jc, time.Minute, pb.ListRequest{
		Selector: pb.LabelSelector{
			Include: pb.MustLabelSet("topic", chunksTopicLabel),
		}}); err != nil {
		return nil, err
	} else {
		return message.ModuloMapping(func(m message.Message, b []byte) []byte {
			return append(b, m.(*Chunk).ID[:]...)
		}, chunkParts.List), nil
	}
}

// finalSumsMapping is a MappingFunc to FinalSumsJournal.
var finalSumsMapping message.MappingFunc = func(msg message.Message) (pb.Journal, message.Framing, error) {
	return FinalSumsJournal, message.JSONFraming, nil
}

// fillPRNG generates fast but high-quality random entropy into the provided byte slice.
func fillPRNG(b []byte) []byte {
	var stream = prngSource.Get().(cipher.Stream)
	stream.XORKeyStream(b[:], b[:])
	prngSource.Put(stream)
	return b
}

var prngSource = sync.Pool{
	New: func() interface{} {
		var key [32]byte
		var iv [aes.BlockSize]byte

		// Generate a random AES key and initialization vector.
		for _, b := range [][]byte{key[:], iv[:]} {
			if _, err := io.ReadFull(rand.Reader, b); err != nil {
				panic(err) // rand.Reader should never error.
			}
		}
		if aesCipher, err := aes.NewCipher(key[:]); err != nil {
			panic(err) // Should never error (given correct |key| size).
		} else {
			return cipher.NewCTR(aesCipher, iv[:])
		}
	},
}

func prngStreamID() (id StreamID) {
	fillPRNG(id[:])
	return
}

func prngInt(N int) int {
	var b [4]byte
	fillPRNG(b[:])
	return int(binary.LittleEndian.Uint32(b[:])) % N
}

var ecmaTable = crc64.MakeTable(crc64.ECMA)
