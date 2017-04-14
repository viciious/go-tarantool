package tarantool

import (
	"bytes"
	"io"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type Packet struct {
	code      int
	requestID uint32
	request   interface{}
	result    *Result
}

func msgpackDecodeBody(d *msgpack.Decoder) ([][]interface{}, error) {
	n, err := d.DecodeSliceLen()
	if err != nil {
		return nil, err
	}

	if n == -1 {
		return nil, nil
	}

	s := make([][]interface{}, n)
	for i := 0; i < n; i++ {
		v, err := d.DecodeSlice()
		if err != nil {
			return nil, err
		}
		s[i] = v
	}

	return s, nil
}

func (pack *Packet) decodeHeader(r io.Reader) (err error) {
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
		default:
			if _, err = d.DecodeInterface(); err != nil {
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
		pack.request = q
		return nil
	}

	unpackr := func(errorCode int) error {
		res := &Result{ErrorCode: errorCode}
		if err := res.unpack(r); err != nil {
			return err
		}
		pack.result = res
		return nil
	}

	if pack.code&ErrorFlag != 0 {
		// error
		return unpackr(pack.code - ErrorFlag)
	}

	switch byte(pack.code) {
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
	default:
		return unpackr(OkCode)
	}

	return
}

func decodePacket(pp *packedPacket) (*Packet, error) {
	r := bytes.NewBuffer(pp.body)

	pack := &Packet{}
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
