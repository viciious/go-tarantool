package tarantool

import (
	"errors"
	"io"

	"github.com/vmihailenco/msgpack"
)

// Eval query
type Eval struct {
	Expression string
	Tuple      []interface{}
}

var _ Query = (*Eval)(nil)

// Pack implements Query interface.
func (q *Eval) Pack(data *packData, w io.Writer) (uint32, error) {
	var err error

	encoder := msgpack.NewEncoder(w)

	encoder.EncodeMapLen(2) // Expression, Tuple

	// Expression
	encoder.EncodeUint(KeyExpression)
	encoder.EncodeString(q.Expression)

	if q.Tuple != nil {
		encoder.EncodeUint(KeyTuple)
		encoder.EncodeArrayLen(len(q.Tuple))
		for _, key := range q.Tuple {
			if err = encoder.Encode(key); err != nil {
				return ErrorFlag, err
			}
		}
	} else {
		encoder.EncodeUint(KeyTuple)
		encoder.EncodeArrayLen(0)
	}

	return EvalRequest, nil
}

// Unpack implements Query interface.
func (q *Eval) Unpack(r io.Reader) (err error) {
	var i int
	var k int

	decoder := msgpack.NewDecoder(r)

	if i, err = decoder.DecodeMapLen(); err != nil {
		return
	}

	if i != 2 {
		return errors.New("Eval.Unpack: expected map of length 2")
	}

	for ; i > 0; i-- {
		if k, err = decoder.DecodeInt(); err != nil {
			return
		}

		switch k {
		case KeyExpression:
			if q.Expression, err = decoder.DecodeString(); err != nil {
				return
			}
		case KeyTuple:
			q.Tuple, err = decoder.DecodeSlice()
			if err != nil {
				return
			}
			if len(q.Tuple) == 0 {
				q.Tuple = nil
			}
		}
	}

	return nil
}
