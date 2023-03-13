package tarantool

import (
	"fmt"
	"time"

	"github.com/tinylib/msgp/msgp"
)

type Packet struct {
	Cmd        uint
	LSN        uint64
	requestID  uint64
	SchemaID   uint64
	InstanceID uint32
	Timestamp  time.Time
	Request    Query
	Result     *Result
}

func (pack *Packet) String() string {
	switch {
	// response to client
	case pack.Result != nil:
		return fmt.Sprintf("Packet Type:%v, ReqID:%v\n%v",
			pack.Cmd, pack.requestID, pack.Result)
	// request to server
	case pack.requestID != 0:
		return fmt.Sprintf("Packet Type:%v, ReqID:%v\nRequest:%#v",
			pack.Cmd, pack.requestID, pack.Request)
	// response from master
	case pack.LSN != 0:
		return fmt.Sprintf("Packet LSN:%v, InstanceID:%v, Timestamp:%v\nRequest:%#v",
			pack.LSN, pack.InstanceID, pack.Timestamp.Format(time.RFC3339), pack.Request)
	default:
		return fmt.Sprintf("Packet %#v", pack)
	}
}

func (pack *Packet) UnmarshalBinaryHeader(data []byte) (buf []byte, err error) {
	var l uint32

	buf = data
	if l, buf, err = msgp.ReadMapHeaderBytes(buf); err != nil {
		return
	}

	for ; l > 0; l-- {
		var cd uint

		if cd, buf, err = msgp.ReadUintBytes(buf); err != nil {
			return
		}

		switch cd {
		case KeySync:
			if pack.requestID, buf, err = msgp.ReadUint64Bytes(buf); err != nil {
				return
			}
		case KeyCode:
			if pack.Cmd, buf, err = msgp.ReadUintBytes(buf); err != nil {
				return
			}
		case KeySchemaID:
			if pack.SchemaID, buf, err = msgp.ReadUint64Bytes(buf); err != nil {
				return
			}
		case KeyLSN:
			if pack.LSN, buf, err = msgp.ReadUint64Bytes(buf); err != nil {
				return
			}
		case KeyInstanceID:
			if pack.InstanceID, buf, err = msgp.ReadUint32Bytes(buf); err != nil {
				return
			}
		case KeyTimestamp:
			var ts float64
			if ts, buf, err = msgp.ReadFloat64Bytes(buf); err != nil {
				return
			}
			ts = ts * 1e9
			pack.Timestamp = time.Unix(0, int64(ts))
		default:
			if buf, err = msgp.Skip(buf); err != nil {
				return
			}
		}
	}
	return buf, nil
}

func (pack *Packet) UnmarshalBinaryBody(data []byte) (buf []byte, err error) {
	unpackq := func(q Query, data []byte) (buf []byte, err error) {
		buf = data
		if buf, err = q.(msgp.Unmarshaler).UnmarshalMsg(buf); err != nil {
			return
		}
		pack.Request = q
		return
	}

	unpackr := func(errorCode uint, data []byte) (buf []byte, err error) {
		buf = data
		res := &Result{ErrorCode: errorCode}
		if buf, err = res.UnmarshalMsg(buf); err != nil {
			return
		}
		pack.Result = res
		return
	}

	if pack.Cmd&ErrorFlag != 0 {
		// error
		return unpackr(pack.Cmd^ErrorFlag, data)
	}

	if q := NewQuery(pack.Cmd); q != nil {
		return unpackq(q, data)
	}
	return unpackr(OKCommand, data)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (pack *Packet) UnmarshalBinary(data []byte) error {
	_, err := pack.UnmarshalMsg(data)
	return err
}

// UnmarshalMsg implements msgp.Unmarshaler
func (pack *Packet) UnmarshalMsg(data []byte) (buf []byte, err error) {
	*pack = Packet{}

	buf = data

	if buf, err = pack.UnmarshalBinaryHeader(buf); err != nil {
		return
	}

	if buf, err = pack.UnmarshalBinaryBody(buf); err != nil {
		return
	}
	return
}
