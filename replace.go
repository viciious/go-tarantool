package tarantool

import (
	"errors"
	"io"

	"github.com/vmihailenco/msgpack"
)

type Replace struct {
	Space interface{}
	Tuple []interface{}
}

var _ Query = (*Replace)(nil)

func (q *Replace) Pack(data *packData, w io.Writer) (uint32, error) {
	var err error

	if q.Tuple == nil {
		return ErrorFlag, errors.New("Tuple can not be nil")
	}

	encoder := msgpack.NewEncoder(w)

	encoder.EncodeMapLen(2) // Space, Tuple

	// Space
	if err = data.writeSpace(q.Space, w, encoder); err != nil {
		return ErrorFlag, err
	}

	// Tuple
	encoder.EncodeUint(KeyTuple)
	encoder.EncodeArrayLen(len(q.Tuple))
	for _, value := range q.Tuple {
		if err = encoder.Encode(value); err != nil {
			return ErrorFlag, err
		}
	}

	return ReplaceRequest, nil
}

func (q *Replace) Unpack(r io.Reader) (err error) {
	var i int
	var k int
	var t uint

	q.Space = nil
	q.Tuple = nil

	decoder := msgpack.NewDecoder(r)
	decoder.UseDecodeInterfaceLoose(true)

	if i, err = decoder.DecodeMapLen(); err != nil {
		return
	}

	for ; i > 0; i-- {
		if k, err = decoder.DecodeInt(); err != nil {
			return
		}

		switch k {
		case KeySpaceNo:
			if t, err = decoder.DecodeUint(); err != nil {
				return
			}
			q.Space = int(t)
		case KeyTuple:
			q.Tuple, err = decoder.DecodeSlice()
			if err != nil {
				return err
			}
		}
	}

	if q.Space == nil {
		return errors.New("Replace.Unpack: no space specified")
	}
	if q.Tuple == nil {
		return errors.New("Replace.Unpack: no tuple specified")
	}

	return nil
}
