package tarantool

import (
	"bytes"
	"errors"
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
		if err = r.Error; err == nil {
			err = errors.New("Unknown error")
		}
		var str = err.Error()

		pp = packIprotoError(r.ErrorCode, requestID)
		encoder := msgpack.NewEncoder(pp.poolBuffer.buffer)
		encoder.EncodeMapLen(1)
		encoder.EncodeUint8(KeyError)
		encoder.EncodeString(str)
	} else {
		pp = packIproto(OkCode, requestID)
		encoder := msgpack.NewEncoder(pp.poolBuffer.buffer)
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

		b := pp.poolBuffer.buffer.Bytes()
		if r.Data != nil {
			packBigTo(uint(len(r.Data)), 4, b[3:])
		} else {
			packBigTo(uint(0), 4, b[3:])
		}
	}

	return pp, nil
}

func (r *Result) unpack(b *bytes.Buffer) (err error) {
	var l int
	d := msgpack.NewDecoder(b)
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
				r.Data[i] = v[i].([]interface{})
			}
		case KeyError:
			errorMessage, err := d.DecodeString()
			if err != nil {
				return err
			}
			r.Error = NewQueryError(errorMessage)
		default:
			if _, err = d.DecodeInterface(); err != nil {
				return
			}
		}
	}
	return
}
