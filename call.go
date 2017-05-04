package tarantool

import (
	"errors"
	"io"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type Call struct {
	Name  string
	Tuple []interface{}
}

var _ Query = (*Call)(nil)

func (q *Call) Pack(data *packData, w io.Writer) (byte, error) {
	var err error

	encoder := msgpack.NewEncoder(w)

	encoder.EncodeMapLen(2) // Name, Tuple

	// Name
	encoder.EncodeUint32(KeyFunctionName)
	encoder.EncodeString(q.Name)

	if q.Tuple != nil {
		encoder.EncodeUint32(KeyTuple)
		encoder.EncodeArrayLen(len(q.Tuple))
		for _, key := range q.Tuple {
			if err = encoder.Encode(key); err != nil {
				return byte(0), err
			}
		}
	} else {
		encoder.EncodeUint32(KeyTuple)
		encoder.EncodeArrayLen(0)
	}

	return CallRequest, nil
}

func (q *Call) Unpack(r io.Reader) (err error) {
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
