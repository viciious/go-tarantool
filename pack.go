package tarantool

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"gopkg.in/vmihailenco/msgpack.v2/codes"
)

var emptyBody = []byte{codes.FixedMapLow}

type packedPacket struct {
	code      uint32
	requestID uint32
	body      []byte // for incoming packets
	buffer    bytes.Buffer
	pool      *packedPacketPool
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
		codes.Uint32, 0, 0, 0, 0, // length
		0x82,                              // 2 element map (codes.FixedMapLow+2)
		KeyCode, codes.Uint32, 0, 0, 0, 0, // code
		KeySync, codes.Uint32, 0, 0, 0, 0,
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

	switch {
	case h[0] <= codes.PosFixedNumHigh:
		bodyLength = int(h[0])
	case h[0] == codes.Uint8:
		if _, err = io.ReadAtLeast(r, h[1:2], 1); err != nil {
			return nil, err
		}
		bodyLength = int(h[1])
	case h[0] == codes.Uint16:
		if _, err = io.ReadAtLeast(r, h[1:3], 2); err != nil {
			return nil, err
		}
		bodyLength = int(binary.BigEndian.Uint16(h[1:3]))
	case h[0] == codes.Uint32:
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
