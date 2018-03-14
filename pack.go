package tarantool

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/vmihailenco/msgpack"
	"github.com/vmihailenco/msgpack/codes"
)

var emptyBody = []byte{byte(codes.FixedMapLow)}

type packedPacket struct {
	code           uint32
	requestID      uint32
	body           []byte // for incoming packets
	buffer         bytes.Buffer
	pool           *packedPacketPool
	packet         Packet
	bodyIndex      int64 // current reading index
	msgpackDecoder *msgpack.Decoder
}

// Uint32 is an alias for PackL
func Uint32(value uint32) []byte {
	result := make([]byte, 4)
	binary.LittleEndian.PutUint32(result, value)
	return result
}

// Uint64 is an alias for PackQ (returns unpacked uint64 bytes in little-endian order)
func Uint64(value uint64) []byte {
	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, value)
	return result
}

func packIproto(code int, requestID uint32) *packedPacket {
	pp := packetPool.Get()
	pp.requestID = requestID
	pp.code = uint32(code)
	return pp
}

func packIprotoError(errCode int, requestID uint32) *packedPacket {
	return packIproto(ErrorFlag|errCode, requestID)
}

func packIprotoOk(requestID uint32) *packedPacket {
	pp := packIproto(OKRequest, requestID)
	pp.buffer.Write(emptyBody)
	return pp
}

func (pp *packedPacket) WriteTo(w io.Writer) (n int64, err error) {

	h32 := [...]byte{
		byte(codes.Uint32), 0, 0, 0, 0, // length
		0x82,                                    // 2 element map (codes.FixedMapLow+2)
		KeyCode, byte(codes.Uint32), 0, 0, 0, 0, // code
		KeySync, byte(codes.Uint32), 0, 0, 0, 0,
	}
	h := h32[:]

	binary.BigEndian.PutUint32(h[8:], pp.code)
	binary.BigEndian.PutUint32(h[14:], pp.requestID)

	body := pp.buffer.Bytes()
	l := len(h) - PacketLengthBytes + len(body)
	binary.BigEndian.PutUint32(h[1:], uint32(l))

	m, err := w.Write(h)
	n += int64(m)
	if err != nil {
		return
	}

	m, err = w.Write(body)
	n += int64(m)

	return
}

func (pp *packedPacket) Release() {
	if pp.pool != nil {
		pp.pool.Put(pp)
	}
}

func readPacked(r io.Reader) (*packedPacket, error) {
	var err error
	var h [PacketLengthBytes]byte
	var bodyLength int

	if _, err = io.ReadAtLeast(r, h[:1], 1); err != nil {
		return nil, err
	}

	c := codes.Code(h[0])
	switch {
	case c <= codes.PosFixedNumHigh:
		bodyLength = int(h[0])
	case c == codes.Uint8:
		if _, err = io.ReadAtLeast(r, h[1:2], 1); err != nil {
			return nil, err
		}
		bodyLength = int(h[1])
	case c == codes.Uint16:
		if _, err = io.ReadAtLeast(r, h[1:3], 2); err != nil {
			return nil, err
		}
		bodyLength = int(binary.BigEndian.Uint16(h[1:3]))
	case c == codes.Uint32:
		if _, err = io.ReadAtLeast(r, h[1:5], 4); err != nil {
			return nil, err
		}
		bodyLength = int(binary.BigEndian.Uint32(h[1:5]))
	default:
		return nil, fmt.Errorf("Wrong packet header: %#v", h)
	}

	if bodyLength == 0 {
		return nil, errors.New("Packet should not be 0 length")
	}

	pp := packetPool.Get()
	pp.buffer.Grow(bodyLength)
	pp.body = pp.buffer.Bytes()[:bodyLength]

	_, err = io.ReadAtLeast(r, pp.body, bodyLength)
	if err != nil {
		pp.Release()
		return nil, err
	}

	return pp, nil
}

func (pp *packedPacket) Reset() {
	pp.bodyIndex = 0
}

// Read implements the io.Reader interface.
func (pp *packedPacket) Read(b []byte) (n int, err error) {
	if pp.bodyIndex >= int64(len(pp.body)) {
		return 0, io.EOF
	}
	n = copy(b, pp.body[pp.bodyIndex:])
	pp.bodyIndex += int64(n)
	return
}

// ReadByte implements the io.ByteReader interface.
func (pp *packedPacket) ReadByte() (byte, error) {
	if pp.bodyIndex >= int64(len(pp.body)) {
		return 0, io.EOF
	}
	b := pp.body[pp.bodyIndex]
	pp.bodyIndex++
	return b, nil
}

// UnreadByte complements ReadByte in implementing the io.ByteScanner interface.
func (pp *packedPacket) UnreadByte() error {
	if pp.bodyIndex <= 0 {
		return errors.New("bytes.Reader.UnreadByte: at beginning of slice")
	}
	pp.bodyIndex--
	return nil
}
