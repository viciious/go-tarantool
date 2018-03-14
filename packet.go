package tarantool

import (
	"fmt"
	"time"

	"github.com/tinylib/msgp/msgp"
)

type Packet struct {
	code       int
	LSN        int64
	requestID  uint32
	InstanceID uint32
	Timestamp  time.Time
	Request    Query
	Result     *Result
}

var emptyPacket Packet

func (pack *Packet) String() string {
	switch {
	// response to client
	case pack.Result != nil:
		return fmt.Sprintf("Packet Type:%v, ReqID:%v\n%v",
			pack.code, pack.requestID, pack.Result)
	// request to server
	case pack.requestID != 0:
		return fmt.Sprintf("Packet Type:%v, ReqID:%v\nRequest:%#v",
			pack.code, pack.requestID, pack.Request)
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
		var cd int
		if cd, buf, err = msgp.ReadIntBytes(buf); err != nil {
			return
		}
		switch cd {
		case KeySync:
			if pack.requestID, buf, err = msgp.ReadUint32Bytes(buf); err != nil {
				return
			}
		case KeyCode:
			if pack.code, buf, err = msgp.ReadIntBytes(buf); err != nil {
				return
			}
		case KeySchemaID:
			if _, buf, err = msgp.ReadUint32Bytes(buf); err != nil {
				return
			}
		case KeyLSN:
			if pack.LSN, buf, err = msgp.ReadInt64Bytes(buf); err != nil {
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
		if buf, err = q.UnmarshalMsg(buf); err != nil {
			return
		}
		pack.Request = q
		return
	}

	unpackr := func(errorCode int, data []byte) (buf []byte, err error) {
		buf = data
		res := &Result{ErrorCode: errorCode}
		if buf, err = res.UnmarshalMsg(buf); err != nil {
			return
		}
		pack.Result = res
		return
	}

	if pack.code&ErrorFlag != 0 {
		// error
		return unpackr(pack.code-ErrorFlag, data)
	}

	switch pack.code {
	case SelectRequest:
		return unpackq(&Select{}, data)
	case AuthRequest:
		return unpackq(&Auth{}, data)
	case InsertRequest:
		return unpackq(&Insert{}, data)
	case ReplaceRequest:
		return unpackq(&Replace{}, data)
	case DeleteRequest:
		return unpackq(&Delete{}, data)
	case CallRequest:
		return unpackq(&Call{}, data)
	case UpdateRequest:
		return unpackq(&Update{}, data)
	case UpsertRequest:
		return unpackq(&Upsert{}, data)
	case PingRequest:
		return unpackq(&Ping{}, data)
	case EvalRequest:
		return unpackq(&Eval{}, data)
	default:
		return unpackr(OkCode, data)
	}
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (pack *Packet) UnmarshalBinary(data []byte) error {
	_, err := pack.UnmarshalMsg(data)
	return err
}

// UnmarshalMsg implements msgp.Unmarshaller
func (pack *Packet) UnmarshalMsg(data []byte) (buf []byte, err error) {
	*pack = emptyPacket

	buf = data
	buf, err = pack.UnmarshalBinaryHeader(data)
	if err != nil {
		return
	}

	buf, err = pack.UnmarshalBinaryBody(buf)
	if err != nil {
		return
	}
	return
}
