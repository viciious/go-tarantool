package tarantool

import (
	"fmt"

	"github.com/tinylib/msgp/msgp"
)

// VClock response (in OK).
// Similar to Result struct
type VClock struct {
	RequestID  uint64 // RequestID is SYNC field;
	InstanceID uint32
	VClock     VectorClock
}

// String implements Stringer interface.
func (p *VClock) String() string {
	return fmt.Sprintf("VClock ReqID:%v Replica:%v, VClock:%#v",
		p.RequestID, p.InstanceID, p.VClock)
}

// UnmarshalMsg implements msgp.Unmarshaller
func (p *VClock) UnmarshalMsg(data []byte) (buf []byte, err error) {
	buf = data
	if buf, err = p.UnmarshalBinaryHeader(buf); err != nil {
		return buf, err
	}
	if len(buf) == 0 {
		return buf, nil
	}
	return p.UnmarshalBinaryBody(buf)
}

func (p *VClock) UnmarshalBinaryHeader(data []byte) (buf []byte, err error) {
	var i uint32

	buf = data
	if i, buf, err = msgp.ReadMapHeaderBytes(buf); err != nil {
		return
	}

	for ; i > 0; i-- {
		var key uint

		if key, buf, err = msgp.ReadUintBytes(buf); err != nil {
			return
		}

		switch key {
		case KeySync:
			if p.RequestID, buf, err = msgp.ReadUint64Bytes(buf); err != nil {
				return
			}
		case KeyInstanceID:
			if p.InstanceID, buf, err = msgp.ReadUint32Bytes(buf); err != nil {
				return
			}
		default:
			if buf, err = msgp.Skip(buf); err != nil {
				return
			}
		}
	}
	return
}

func (p *VClock) UnmarshalBinaryBody(data []byte) (buf []byte, err error) {
	var count uint32

	buf = data
	if count, buf, err = msgp.ReadMapHeaderBytes(buf); err != nil {
		return
	}

	for ; count > 0; count-- {
		var key uint

		if key, buf, err = msgp.ReadUintBytes(buf); err != nil {
			return
		}
		switch key {
		case KeyVClock:
			var n uint32
			var id uint32
			var lsn uint64

			if n, buf, err = msgp.ReadMapHeaderBytes(buf); err != nil {
				return
			}
			p.VClock = NewVectorClock()
			for ; n > 0; n-- {
				if id, buf, err = msgp.ReadUint32Bytes(buf); err != nil {
					return
				}
				if lsn, buf, err = msgp.ReadUint64Bytes(buf); err != nil {
					return
				}
				if !p.VClock.Follow(id, lsn) {
					return buf, ErrVectorClock
				}
			}
		default:
			if buf, err = msgp.Skip(buf); err != nil {
				return
			}
		}
	}
	return
}
