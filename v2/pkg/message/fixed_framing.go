package message

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/pkg/errors"
)

// FixedFraming is a Framing implementation which encodes messages in a binary
// format with a fixed-length header. Messages must support Size and MarshalTo
// functions for marshal support (eg, generated Protobuf messages satisfy this
// interface). Messages are encoded as a 4-byte magic word for de-synchronization
// detection, followed by a little-endian uint32 length, followed by payload bytes.
var FixedFraming = new(fixedFraming)

// FixedFrameHeaderLength is the number of leading header bytes of each frame:
// A 4-byte magic word followed by a little-endian length.
const FixedFrameHeaderLength = 8

type fixedFraming struct{}

// Name returns protocol.FramingFixed.
func (f *fixedFraming) Name() string { return protocol.FramingFixed }

// Marshal implements Framing. It returns an error only if Message.Encode fails.
func (f *fixedFraming) Marshal(msg Message, bw *bufio.Writer) error {
	var b, err = f.Encode(msg, bufferPool.Get().([]byte))
	if err == nil {
		bw.Write(b)
	}
	bufferPool.Put(b[:0])
	return err
}

// Encode a Message by appending into buffer |b|, which will be grown if needed and returned.
func (*fixedFraming) Encode(msg Message, b []byte) ([]byte, error) {
	var p, ok = msg.(interface {
		ProtoSize() int
		MarshalTo([]byte) (int, error)
	})
	if !ok {
		return nil, fmt.Errorf("%+v is not fixed-frameable (must implement ProtoSize and MarshalTo)", msg)
	}

	var size = FixedFrameHeaderLength + p.ProtoSize()
	var offset = len(b)

	if size > (cap(b) - offset) {
		b = append(b, make([]byte, size)...)
	} else {
		b = b[:offset+size]
	}

	// Header consists of a magic word (for de-sync detection), and a 4-byte length.
	copy(b[offset:offset+4], magicWord[:])
	binary.LittleEndian.PutUint32(b[offset+4:offset+8], uint32(size-FixedFrameHeaderLength))

	if _, err := p.MarshalTo(b[offset+FixedFrameHeaderLength:]); err != nil {
		return nil, err
	}
	return b, nil
}

// Unpack returns the next fixed frame of content from the Reader, including
// the frame header. If the magic word is not detected (indicating a desync),
// Unpack attempts to continue reading until the next magic word, returning
// the interleaved but desynchronized content.
//
// It implements Framing.
func (*fixedFraming) Unpack(r *bufio.Reader) ([]byte, error) {
	var b, err = r.Peek(FixedFrameHeaderLength)

	if err != nil {
		// If buffer just contains a trailing newline, return EOF.
		// TODO(johnny): Can we remove this?
		if err == io.EOF && len(b) == 1 && b[0] == 0x0a {
			return nil, io.EOF
		}
		if err == io.EOF && len(b) != 0 {
			// If we read at least one byte, then an EOF is unexpected (it should
			// occur only on whole-message boundaries).
			err = io.ErrUnexpectedEOF
		} else {
			err = errors.Wrap(err, "Peek(FixedFrameHeaderLength)")
		}
		return nil, err
	}

	if !matchesMagicWord(b) {
		// We are not at the expected frame boundary. Scan forward within the buffered
		// region to the beginning of the next magic word. Return the intermediate
		// jumbled frame (this will produce an ErrDesyncDetected on a later Unmarshal).
		b, _ = r.Peek(r.Buffered())

		var i, j = 1, 1 + len(b) - len(magicWord)
		for ; i != j; i++ {
			if matchesMagicWord(b[i:]) {
				break
			}
		}
		r.Discard(i)
		return b[:i], nil
	}

	// Next 4 bytes are encoded size. Combine with header for full frame size.
	var size = FixedFrameHeaderLength + int(binary.LittleEndian.Uint32(b[4:]))

	// Fast path: check if the full frame is available in buffer. Return the
	// buffer internal slice without copying. It is invalidated by the next
	// Unpack (or other Reader operation).
	if b, err = r.Peek(size); err == nil {
		r.Discard(size)
		return b, nil
	}

	// Slow path. Allocate and attempt to Read the full frame.
	b = make([]byte, size)
	_, err = io.ReadFull(r, b)
	return b, errors.Wrap(err, "io.ReadFull")
}

// Unmarshal verifies the frame header and unpacks Message content. If the frame
// header indicates a desync occurred (incorrect magic word), ErrDesyncDetected
// is returned.
//
// It implements Framing.
func (*fixedFraming) Unmarshal(b []byte, msg Message) error {
	var p, ok = msg.(interface {
		Unmarshal([]byte) error
	})

	if !ok {
		return fmt.Errorf("%+v is not fixed-frameable (must implement Unmarshal)", msg)
	} else if !matchesMagicWord(b) {
		return ErrDesyncDetected
	} else if err := p.Unmarshal(b[FixedFrameHeaderLength:]); err != nil {
		return err
	} else if f, ok := msg.(Fixupable); ok {
		return f.Fixup()
	}
	return nil
}

func matchesMagicWord(b []byte) bool {
	return b[0] == magicWord[0] && b[1] == magicWord[1] && b[2] == magicWord[2] && b[3] == magicWord[3]
}

var (
	// ErrDesyncDetected is returned by Unmarshal upon detection of an invalid frame.
	ErrDesyncDetected = errors.New("detected de-synchronization")
	// magicWord precedes all fixedFraming encodings.
	magicWord = [4]byte{0x66, 0x33, 0x93, 0x36}
	// bufferPool pools buffers used for MarshalTo encodings.
	bufferPool = sync.Pool{New: func() interface{} { return make([]byte, 0, 1024) }}
)
