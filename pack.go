package tarantool

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var emptyBody = []byte{0x80}

type packedPacket struct {
	code      uint32
	requestID uint32
	body      []byte // for incoming packets
	buffer    bytes.Buffer
	pool      *packedPacketPool
}

func packLittle(value uint, bytes int) []byte {
	b := value
	result := make([]byte, bytes)
	for i := 0; i < bytes; i++ {
		result[i] = uint8(b & 0xFF)
		b >>= 8
	}
	return result
}

func packBig(value uint, bytes int) []byte {
	b := value
	result := make([]byte, bytes)
	for i := bytes - 1; i >= 0; i-- {
		result[i] = uint8(b & 0xFF)
		b >>= 8
	}
	return result
}

func packLittleTo(value uint, bytes int, dest []byte) {
	b := value
	for i := 0; i < bytes; i++ {
		dest[i] = uint8(b & 0xFF)
		b >>= 8
	}
}

func packBigTo(value uint, bytes int, dest []byte) {
	b := value
	for i := bytes - 1; i >= 0; i-- {
		dest[i] = uint8(b & 0xFF)
		b >>= 8
	}
}

// Uint32 is an alias for PackL
func Uint32(value uint32) []byte {
	return packLittle(uint(value), 4)
}

// Uint64 is an alias for PackQ (returns unpacked uint64 btyes in little-endian order)
func Uint64(value uint64) []byte {
	b := value
	result := make([]byte, 8)
	for i := 0; i < 8; i++ {
		result[i] = uint8(b & 0xFF)
		b >>= 8
	}
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

	if h[0] <= 0x7f {
		bodyLength = int(h[0])
	} else if h[0] == 0xcc {
		if _, err = io.ReadAtLeast(r, h[1:2], 1); err != nil {
			return nil, err
		}
		bodyLength = int(h[1])
	} else if h[0] == 0xcd {
		if _, err = io.ReadAtLeast(r, h[1:3], 2); err != nil {
			return nil, err
		}
		bodyLength = int(binary.BigEndian.Uint16(h[1:3]))
	} else if h[0] == 0xce {
		if _, err = io.ReadAtLeast(r, h[1:5], 4); err != nil {
			return nil, err
		}
		bodyLength = int(binary.BigEndian.Uint32(h[1:5]))
	} else {
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
