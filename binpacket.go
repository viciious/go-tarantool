package tarantool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/tinylib/msgp/msgp"
)

type binaryPacket struct {
	body   []byte
	pool   *binaryPacketPool
	packet Packet
}

// WriteTo implements the io.WriterTo interface
func (pp *binaryPacket) WriteTo(w io.Writer) (n int64, err error) {
	h32 := [32]byte{0xce, 0, 0, 0, 0}

	h := h32[5:5]
	body := pp.body

	h = msgp.AppendMapHeader(h, 2)
	h = msgp.AppendUint(h, KeyCode)
	h = msgp.AppendUint32(h, pp.packet.cmd)
	h = msgp.AppendUint(h, KeySync)
	h = msgp.AppendUint64(h, pp.packet.requestID)

	l := len(h) + len(body)
	h = h32[:5+len(h)]
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

func (pp *binaryPacket) Reset() {
	pp.packet.cmd = OKCommand
	pp.packet.requestID = 0
	pp.body = pp.body[:0]
}

func (pp *binaryPacket) Release() {
	if pp.pool != nil {
		pp.pool.Put(pp)
	}
}

// ReadFrom implements the io.ReaderFrom interface
func (pp *binaryPacket) ReadFrom(r io.Reader) (n int64, err error) {
	var h [8]byte
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

func (pp *binaryPacket) ReadPacket(r io.Reader) (err error) {
	if _, err = pp.ReadFrom(r); err != nil {
		return
	}

	return pp.packet.UnmarshalBinary(pp.body)
}

func (pp *binaryPacket) packQuery(q Query, packdata *packData) (err error) {
	if pp.body, err = q.PackMsg(packdata, pp.body[:0]); err != nil {
		pp.packet.cmd = ErrorFlag
		return err
	}
	pp.packet.cmd = q.GetCommandID()
	return nil
}
