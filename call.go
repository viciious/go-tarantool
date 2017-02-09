package tarantool

import (
	"bytes"
	"errors"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type Call struct {
	Name  string
	Tuple []interface{}
}

var _ Query = (*Call)(nil)

func (s *Call) Pack(data *packData) (byte, []byte, error) {
	var bodyBuffer bytes.Buffer
	var err error

	encoder := msgpack.NewEncoder(&bodyBuffer)

	encoder.EncodeMapLen(2) // Name, Tuple

	// Name
	encoder.EncodeUint32(KeyFunctionName)
	encoder.EncodeString(s.Name)

	if s.Tuple != nil {
		encoder.EncodeUint32(KeyTuple)
		encoder.EncodeArrayLen(len(s.Tuple))
		for _, key := range s.Tuple {
			if err = encoder.Encode(key); err != nil {
				return byte(0), nil, err
			}
		}
	} else {
		encoder.EncodeUint32(KeyTuple)
		encoder.EncodeArrayLen(0)
	}

	return CallRequest, bodyBuffer.Bytes(), nil
}

func (q *Call) Unpack(r *bytes.Buffer) (err error) {
	var i int
	var k int

	q.Name = ""
	q.Tuple = nil

	decoder := msgpack.NewDecoder(r)

	if i, err = decoder.DecodeMapLen(); err != nil {
		return
	}

	if i != 2 {
		return errors.New("Call.Unpack: expected map of length 2")
	}

	for ; i > 0; i-- {
		if k, err = decoder.DecodeInt(); err != nil {
			return
		}

		switch k {
		case KeyFunctionName:
			if q.Name, err = decoder.DecodeString(); err != nil {
				return
			}
		case KeyTuple:
			q.Tuple, err = decoder.DecodeSlice()
			if err != nil {
				return err
			}
			if len(q.Tuple) == 0 {
				q.Tuple = nil
			}
		}
	}

	if q.Name == "" {
		return errors.New("Call.Unpack: no space specified")
	}

	return nil
}
