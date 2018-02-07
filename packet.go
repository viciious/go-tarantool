package tarantool

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/vmihailenco/msgpack"
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

func (pack *Packet) decodeHeader(r io.Reader) (err error) {
	var l int
	d := msgpack.NewDecoder(r)
	d.UseDecodeInterfaceLoose(true)
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
			if pack.requestID, err = d.DecodeUint32(); err != nil {
				return
			}
		case KeyCode:
			if pack.code, err = d.DecodeInt(); err != nil {
				return
			}
		case KeySchemaID:
			if _, err = d.DecodeUint32(); err != nil {
				return
			}
		case KeyLSN:
			if pack.LSN, err = d.DecodeInt64(); err != nil {
				return
			}
		case KeyInstanceID:
			if pack.InstanceID, err = d.DecodeUint32(); err != nil {
				return
			}
		case KeyTimestamp:
			var ts float64
			if ts, err = d.DecodeFloat64(); err != nil {
				return
			}
			ts = ts * 1e9
			pack.Timestamp = time.Unix(0, int64(ts))
		default:
			if err = d.Skip(); err != nil {
				return
			}
		}
	}
	return nil
}

func (pack *Packet) decodeBody(r io.Reader) (err error) {
	unpackq := func(q Query) error {
		if err := q.Unpack(r); err != nil {
			return err
		}
		pack.Request = q
		return nil
	}

	unpackr := func(errorCode int) error {
		res := &Result{ErrorCode: errorCode}
		if err := res.unpack(r); err != nil {
			return err
		}
		pack.Result = res
		return nil
	}

	if pack.code&ErrorFlag != 0 {
		// error
		return unpackr(pack.code - ErrorFlag)
	}

	switch pack.code {
	case SelectRequest:
		return unpackq(&Select{})
	case AuthRequest:
		return unpackq(&Auth{})
	case InsertRequest:
		return unpackq(&Insert{})
	case ReplaceRequest:
		return unpackq(&Replace{})
	case DeleteRequest:
		return unpackq(&Delete{})
	case CallRequest:
		return unpackq(&Call{})
	case UpdateRequest:
		return unpackq(&Update{})
	case UpsertRequest:
		return unpackq(&Upsert{})
	case PingRequest:
		return unpackq(&Ping{})
	case EvalRequest:
		return unpackq(&Eval{})
	default:
		return unpackr(OkCode)
	}
}

func decodePacket(pp *packedPacket) (*Packet, error) {
	r := bytes.NewReader(pp.body)

	pack := &pp.packet
	*pack = emptyPacket

	err := pack.decodeHeader(r)
	if err != nil {
		return nil, err
	}

	err = pack.decodeBody(r)
	if err != nil {
		return nil, err
	}
	return pack, nil
}
