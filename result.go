package tarantool

import (
	"encoding/binary"
	"fmt"
	"io"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type Result struct {
	ErrorCode int
	Error     error
	Data      [][]interface{}
}

func (r *Result) pack(requestID uint32) (*packedPacket, error) {
	var err error
	var pp *packedPacket

	if r.ErrorCode != OkCode || r.Error != nil {
		if r.Error == nil {
			err = ErrUnknownError
		}
		var str = err.Error()

		pp = packIprotoError(r.ErrorCode, requestID)
		encoder := msgpack.NewEncoder(&pp.buffer)
		encoder.EncodeMapLen(1)
		encoder.EncodeUint8(KeyError)
		encoder.EncodeString(str)
	} else {
		pp = packIproto(OkCode, requestID)
		encoder := msgpack.NewEncoder(&pp.buffer)
		encoder.EncodeMapLen(1)
		encoder.EncodeUint8(KeyData)
		encoder.EncodeArrayLen(65536) // force encoding as uin32, to be replaced later

		if r.Data != nil {
			for i := 0; i < len(r.Data); i++ {
				if err = encoder.Encode(r.Data[i]); err != nil {
					pp.Release()
					return nil, err
				}
			}
		}

		b := pp.buffer.Bytes()
		if r.Data != nil {
			binary.BigEndian.PutUint32(b[3:], uint32(len(r.Data)))
		} else {
			binary.BigEndian.PutUint32(b[3:], 0)
		}
	}

	return pp, nil
}

func (r *Result) unpack(rr io.Reader) (err error) {
	var l int
	d := msgpack.NewDecoder(rr)
	if l, err = d.DecodeMapLen(); err != nil {
		return
	}
	for ; l > 0; l-- {
		var cd int
		if cd, err = d.DecodeInt(); err != nil {
			return
		}
		switch cd {
		case KeyData:
			value, err := d.DecodeInterface()
			if err != nil {
				return err
			}
			v := value.([]interface{})
			r.Data = make([]([]interface{}), len(v))
			for i := 0; i < len(v); i++ {
				switch vi := v[i].(type) {
				case []interface{}:
					r.Data[i] = vi
				default:
					r.Data[i] = []interface{}{vi}
				}
			}
		case KeyError:
			errorMessage, err := d.DecodeString()
			if err != nil {
				return err
			}
			r.Error = NewQueryError(errorMessage)
		default:
			if err = d.Skip(); err != nil {
				return
			}
		}
	}
	return
}

func (r *Result) String() string {
	switch {
	case r == nil:
		return "Result <nil>"
	case r.Error != nil:
		return fmt.Sprintf("Result ErrCode:%v, Err: %v", r.ErrorCode, r.Error)
	case r.Data != nil:
		return fmt.Sprintf("Result Data:%#v", r.Data)
	default:
		return ""
	}
}
