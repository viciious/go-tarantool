package tarantool

import (
	"bytes"
	"fmt"
	"io"

	"gopkg.in/vmihailenco/msgpack.v2"
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

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (p *VClock) UnmarshalBinary(data []byte) (err error) {
	r := bytes.NewReader(data)
	if err = p.decodeHeader(r); err != nil {
		return err
	}

	if r.Len() == 0 {
		return nil
	}
	return p.decodeBody(r)
}

func (p *VClock) decodeHeader(r io.Reader) (err error) {
	var l int
	d := msgpack.NewDecoder(r)
	if l, err = d.DecodeMapLen(); err != nil {
		return
	}
	for ; l > 0; l-- {
		var cd int
		if cd, err = d.DecodeInt(); err != nil {
			return
		}
		switch cd {
		case KeySync:
			if p.RequestID, err = d.DecodeUint64(); err != nil {
				return
			}
		case KeySchemaID:
			if _, err = d.DecodeUint32(); err != nil {
				return
			}
		case KeyInstanceID:
			if p.InstanceID, err = d.DecodeUint32(); err != nil {
				return
			}
		default:
			if err = d.Skip(); err != nil {
				return
			}
		}
	}
	return nil
}

func (p *VClock) decodeBody(r io.Reader) (err error) {
	var count int
	d := msgpack.NewDecoder(r)
	if count, err = d.DecodeMapLen(); err != nil {
		return
	}

	for ; count > 0; count-- {
		var key int
		if key, err = d.DecodeInt(); err != nil {
			return
		}
		switch key {
		case KeyVClock:
			if p.VClock, err = decodeVectorClock(d); err != nil {
				return
			}
		default:
			if err = d.Skip(); err != nil {
				return
			}
		}
	}
	return
}

var _ Query = (*VClock)(nil)

// Pack implements a part of the Query interface
func (p *VClock) Pack(data *packData, w io.Writer) (uint32, error) {
	var err error

	encoder := msgpack.NewEncoder(w)
	encoder.EncodeMapLen(1)
	if err = encoder.EncodeUint8(uint8(KeyVClock)); err != nil {
		return 0, err
	}
	if err := encoder.EncodeMapLen(len(p.VClock[1:])); err != nil {
		return 0, err
	}
	for i, lsn := range p.VClock[1:] {
		encoder.EncodeUint32(uint32(i))
		encoder.EncodeInt64(lsn)
	}

	return OkCode, nil
}

// Unpack implements a part of the Query interface
func (p *VClock) Unpack(r io.Reader) (err error) {
	return ErrNotSupported
}

func decodeVectorClock(d *msgpack.Decoder) (VClock VectorClock, err error) {
	var n int
	if n, err = d.DecodeMapLen(); err != nil {
		return nil, err
	}
	VClock = NewVectorClock()
	for ; n > 0; n-- {
		id, err := d.DecodeUint32()
		if err != nil {
			return nil, err
		}
		lsn, err := d.DecodeInt64()
		if err != nil {
			return nil, err
		}
		if !VClock.Follow(id, lsn) {
			return nil, ErrVectorClock
		}
	}

	return VClock, nil
}
