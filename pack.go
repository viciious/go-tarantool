package tarantool

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var packedOkBody = []byte{0x80}

type packedPacket struct {
	code      interface{}
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

func packIproto(code interface{}, requestID uint32) *packedPacket {
	pp := packetPool.Get()
	pp.requestID = requestID
	pp.code = code
	return pp
}

func packIprotoError(code int, requestID uint32) *packedPacket {
	return packIproto(ErrorFlag|code, requestID)
}

func packIprotoOk(requestID uint32) *packedPacket {
	pp := packIproto(OkCode, requestID)
	pp.buffer.Write(packedOkBody)
	return pp
}

func (pp *packedPacket) WriteTo(w io.Writer) (n int, err error) {
	h8 := [...]byte{
		0xce, 0, 0, 0, 0, // length
		0x82,       // 2 element map
		KeyCode, 0, // code
		KeySync, 0xce,
		0, 0,
		0, 0,
	}
	h32 := [...]byte{
		0xce, 0, 0, 0, 0, // length
		0x82,                      // 2 element map
		KeyCode, 0xce, 0, 0, 0, 0, // code
		KeySync, 0xce, 0, 0, 0, 0,
	}
	var h []byte

	code := pp.code
	requestID := pp.requestID
	body := pp.buffer.Bytes()

	switch code.(type) {
	case byte:
		h = h8[:]
		h[7] = code.(byte)
		packBigTo(uint(requestID), 4, h[10:])
	case uint:
		h = h32[:]
		packBigTo(code.(uint), 4, h[8:])
		packBigTo(uint(requestID), 4, h[14:])
	case int:
		h = h32[:]
		packBigTo(uint(code.(int)), 4, h[8:])
		packBigTo(uint(requestID), 4, h[14:])
	case uint32:
		h = h32[:]
		packBigTo(uint(code.(uint32)), 4, h[8:])
		packBigTo(uint(requestID), 4, h[14:])
	case int32:
		h = h32[:]
		packBigTo(uint(code.(int32)), 4, h[8:])
		packBigTo(uint(requestID), 4, h[14:])
	default:
		panic("packIproto: unknown code type")
	}

	l := uint(len(h) - 5 + len(body))
	packBigTo(l, 4, h[1:])

	n, err = w.Write(h)
	if err != nil {
		return
	}

	nn, err := w.Write(body)
	if err != nil {
		return n + nn, err
	}
	return n + nn, nil
}

func (pp *packedPacket) Release() {
	if pp.pool != nil {
		pp.pool.Put(pp)
	}
}

func readPacked(r io.Reader) (*packedPacket, error) {
	var err error
	var h [5]byte

	if _, err = io.ReadAtLeast(r, h[:], 5); err != nil {
		return nil, err
	}

	if h[0] != 0xce {
		return nil, fmt.Errorf("Wrong response header: %#v", h)
	}

	bodyLength := int(binary.BigEndian.Uint32(h[1:5]))
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
