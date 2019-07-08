package tarantool

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"gopkg.in/vmihailenco/msgpack.v2"
)

var ErrInvalidBallot = errors.New("Invalid Ballot response")

// Ballot response (in OK) for VoteRequest.
// Tarantool >= 1.9.0.
type Ballot struct {
	RequestID  uint64
	InstanceID uint32

	ReadOnly bool
	VClock   VectorClock
	GCVClock VectorClock
}

func (p *Ballot) String() string {
	return fmt.Sprintf("Ballot ReqID:%v Replica:%v, ReadOnly:%v, VClock:%#v, GCVClock:%#v",
		p.RequestID, p.InstanceID, p.ReadOnly, p.VClock, p.GCVClock)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (p *Ballot) UnmarshalBinary(data []byte) (err error) {
	r := bytes.NewReader(data)
	if err = p.decodeHeader(r); err != nil {
		return err
	}

	if r.Len() == 0 {
		return nil
	}

	return p.decodeBody(r)
}

func (p *Ballot) decodeHeader(r io.Reader) (err error) {
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

func (p *Ballot) decodeBody(r io.Reader) (err error) {
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
		case KeyBallot:
			var l int
			if l, err = d.DecodeMapLen(); err != nil {
				return
			}

			if l < 3 {
				return ErrInvalidBallot
			}

			if _, err = d.DecodeInt(); err != nil {
				return err
			}

			if p.ReadOnly, err = d.DecodeBool(); err != nil {
				return
			}

			if _, err = d.DecodeInt(); err != nil {
				return err
			}

			if p.VClock, err = decodeVectorClock(d); err != nil {
				return err
			}

			if _, err = d.DecodeInt(); err != nil {
				return err
			}

			if p.GCVClock, err = decodeVectorClock(d); err != nil {
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
