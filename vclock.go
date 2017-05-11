package tarantool

import (
	"errors"
	"fmt"

	"io"

	"gopkg.in/vmihailenco/msgpack.v2"
)

// VClock response
type VClock struct {
	RequestID  uint64 // RequestID is SYNC field;
	InstanceID uint32
	VClock     VectorClock
}

// String implements Stringer interface
func (q *VClock) String() string {
	return fmt.Sprintf("VClock ReqID:%v Replica:%v, VClock:%#v",
		q.RequestID, q.InstanceID, q.VClock)
}

// Pack implements a part of the Query interface
func (q *VClock) Pack(r io.Reader) (err error) {
	// TODO: support Subscribe Unpack
	return errors.New("Not supported yet")
}

// Unpack implements a part of the Query interface
func (q *VClock) Unpack(r io.Reader) (err error) {
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
			var n int
			if n, err = d.DecodeMapLen(); err != nil {
				return err
			}
			q.VClock = make(VectorClock, n)
			for ; n > 0; n-- {
				mk, err := d.DecodeUint32()
				if err != nil {
					return err
				}
				mv, err := d.DecodeInt64()
				if err != nil {
					return err
				}
				q.VClock[mk] = mv
			}
		default:
			if err = d.Skip(); err != nil {
				return
			}
		}
	}
	return
}
