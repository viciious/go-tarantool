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
	var bodyBuffer bytes.Buffer
	var h, body []byte
	var he = [...]byte{
		0x81, KeyError, 0xdb, 0, 0, 0, 0,
	}
	var hd = [...]byte{
		0x81, KeyData, 0xdd, 0, 0, 0, 0,
	}

	if r.ErrorCode != OkCode || r.Error != nil {
		h = he[:]
		if err = r.Error; err == nil {
			err = errors.New("Unknown error")
		}
		var str = err.Error()
		packBigTo(uint(len(str)), 4, h[3:])
		body = append(h[:], str...)
		return packIprotoError(r.ErrorCode, requestID, body), nil
	} else {
		h = hd[:]
		body = h[:]
		if r.Data != nil {
			encoder := msgpack.NewEncoder(&bodyBuffer)
			for i := 0; i < len(r.Data); i++ {
				if err = encoder.Encode(r.Data[i]); err != nil {
					return nil, err
				}
			}
			packBigTo(uint(len(r.Data)), 4, h[3:])
			body = append(h[:], bodyBuffer.Bytes()...)
		}
		return packIproto(OkCode, requestID, body), nil
	}
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
