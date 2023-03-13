package tarantool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/tinylib/msgp/msgp"
)

type BinaryPacket struct {
	body   []byte
	header [32]byte
	pool   *BinaryPacketPool
	packet Packet
}

type UnmarshalBinaryBodyFunc func(*Packet, []byte) error

// WriteTo implements the io.WriterTo interface
func (pp *BinaryPacket) WriteTo(w io.Writer) (n int64, err error) {
	h32 := pp.header[:32]
	h32[0], h32[1], h32[2], h32[3], h32[4] = 0xce, 0, 0, 0, 0

	h := h32[5:5]
	body := pp.body

	var ne uint32 = 2
	if pp.packet.SchemaID != 0 {
		ne++
	}
	h = msgp.AppendMapHeader(h, ne)
	h = msgp.AppendUint(h, KeyCode)
	h = msgp.AppendUint(h, pp.packet.Cmd)
	h = msgp.AppendUint(h, KeySync)
	h = msgp.AppendUint64(h, pp.packet.requestID)
	if pp.packet.SchemaID != 0 {
		h = msgp.AppendUint(h, KeySchemaID)
		h = msgp.AppendUint64(h, pp.packet.SchemaID)
	}

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

func (pp *BinaryPacket) Reset() {
	pp.packet.Cmd = OKCommand
	pp.packet.SchemaID = 0
	pp.packet.requestID = 0
	pp.packet.Result = nil
	pp.body = pp.body[:0]
}

func (pp *BinaryPacket) Release() {
	if pp.pool != nil {
		pp.pool.Put(pp)
	}
}

// ReadFrom implements the io.ReaderFrom interface
func (pp *BinaryPacket) ReadFrom(r io.Reader) (n int64, err error) {
	var h = pp.header[:8]
	var bodyLength uint
	var headerLength uint
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
		return int64(rr), fmt.Errorf("wrong packet header: %#v", c)
	}

	if headerLength > 1 {
		crr, err = io.ReadFull(r, h[1:headerLength])
		if rr = rr + crr; err != nil {
			return int64(rr), err
		}
	}

	if bodyLength, _, err = msgp.ReadUintBytes(h[:headerLength]); err != nil {
		return int64(rr), err
	}
	if bodyLength == 0 {
		return int64(rr), errors.New("Packet should not be 0 length")
	}

	if uint(cap(pp.body)) < bodyLength {
		pp.body = make([]byte, bodyLength+bodyLength/2)
	}

	pp.body = pp.body[:bodyLength]
	crr, err = io.ReadFull(r, pp.body)
	return int64(rr) + int64(crr), err
}

func (pp *BinaryPacket) Unmarshal() error {
	if err := pp.packet.UnmarshalBinary(pp.body); err != nil {
		return fmt.Errorf("Error decoding packet type %d: %s", pp.packet.Cmd, err)
	}
	return nil
}

func (pp *BinaryPacket) UnmarshalCustomBody(um UnmarshalBinaryBodyFunc) (err error) {
	buf := pp.body

	if buf, err = pp.packet.UnmarshalBinaryHeader(buf); err != nil {
		return fmt.Errorf("Error decoding packet type %d: %s", pp.packet.Cmd, err)
	}

	if err = um(&pp.packet, buf); err != nil {
		return fmt.Errorf("Error decoding packet type %d: %s", pp.packet.Cmd, err)
	}

	return nil
}

func (pp *BinaryPacket) Bytes() []byte {
	return pp.body
}

func (pp *BinaryPacket) Result() *Result {
	return pp.packet.Result
}

func (pp *BinaryPacket) readPacket(r io.Reader) (err error) {
	if _, err = pp.ReadFrom(r); err != nil {
		return
	}
	return pp.packet.UnmarshalBinary(pp.body)
}

// ReadRawPacket reads the whole packet body and only unpacks request ID for routing purposes
func (pp *BinaryPacket) readRawPacket(r io.Reader) (requestID uint64, err error) {
	var l uint32

	requestID = 0
	if _, err = pp.ReadFrom(r); err != nil {
		return
	}

	buf := pp.body
	if l, buf, err = msgp.ReadMapHeaderBytes(buf); err != nil {
		return
	}

	for ; l > 0; l-- {
		var cd uint
		if cd, buf, err = msgp.ReadUintBytes(buf); err != nil {
			return
		}
		if cd == KeySync {
			requestID, _, err = msgp.ReadUint64Bytes(buf)
			return
		}
		if buf, err = msgp.Skip(buf); err != nil {
			return
		}
	}

	return
}

func (pp *BinaryPacket) packMsg(q Query, packdata *packData) (err error) {
	if iq, ok := q.(internalQuery); ok {
		if pp.body, err = iq.packMsg(packdata, pp.body[:0]); err != nil {
			pp.packet.Cmd = ErrorFlag
			return err
		}
	} else if mp, ok := q.(msgp.Marshaler); ok {
		if pp.body, err = mp.MarshalMsg(pp.body[:0]); err != nil {
			pp.packet.Cmd = ErrorFlag
			return err
		}
	} else {
		pp.packet.Cmd = ErrorFlag
		return errors.New("query struct doesn't implement any known marshalling interface")
	}

	pp.packet.Cmd = q.GetCommandID()
	return nil
}
