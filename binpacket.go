package tarantool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/tinylib/msgp/msgp"
)

var emptyBody = []byte{byte(0)}

type binaryPacket struct {
	code      uint32
	requestID uint32
	body      []byte // for incoming packets
	pool      *binaryPacketPool
	packet    Packet
}

// WriteTo implements the io.WriterTo interface
func (pp *binaryPacket) WriteTo(w io.Writer) (n int64, err error) {
	h32 := [...]byte{
		0xce, 0, 0, 0, 0, // length
		0x82,                      // 2 element map (codes.FixedMapLow+2)
		KeyCode, 0xce, 0, 0, 0, 0, // code
		KeySync, 0xce, 0, 0, 0, 0,
	}
	h := h32[:]

	binary.BigEndian.PutUint32(h[8:], pp.code)
	binary.BigEndian.PutUint32(h[14:], pp.requestID)

	body := pp.body[:]
	l := len(h) - PacketLengthBytes + len(body)
	binary.BigEndian.PutUint32(h[1:], uint32(l))

	m, err := w.Write(h)
	n += int64(m)
	if err != nil {
		return
	}

	m, err = w.Write(body)
	n += int64(m)
	pp.body = pp.body[:0]

	return
}

func (pp *binaryPacket) Release() {
	if pp.pool != nil {
		pp.pool.Put(pp)
	}
}

// ReadFrom implements the io.ReaderFrom interface
func (pp *binaryPacket) ReadFrom(r io.Reader) (n int64, err error) {
	var h [PacketLengthBytes]byte
	var bodyLength int
	var headerLength int
	var rr, crr int

	if rr, err = io.ReadFull(r, h[:1]); err != nil {
		return int64(rr), err
	}

	c := h[0]
	switch {
	case c <= 0x7f:
		headerLength = 1
	case c == 0xcc:
		headerLength = 2
	case c == 0xcd:
		headerLength = 3
	case c == 0xce:
		headerLength = 5
	default:
		return int64(rr), fmt.Errorf("Wrong packet header: %#v", c)
	}

	if headerLength > 1 {
		crr, err = io.ReadFull(r, h[1:headerLength])
		if rr = rr + crr; err != nil {
			return int64(rr), err
		}
	}

	if bodyLength, _, err = msgp.ReadIntBytes(h[:headerLength]); err != nil {
		return int64(rr), err
	}

	if bodyLength == 0 {
		return int64(rr), errors.New("Packet should not be 0 length")
	}

	if cap(pp.body) < bodyLength {
		pp.body = make([]byte, bodyLength+bodyLength/2)
	}

	crr, err = io.ReadFull(r, pp.body[:bodyLength])
	return int64(rr) + int64(crr), err
}

func (pp *binaryPacket) packQuery(q Query, packdata *packData) (err error) {
	pp.body, pp.code, err = q.PackMsg(packdata, pp.body)
	return err
}
