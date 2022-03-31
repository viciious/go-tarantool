package tarantool

import (
	"fmt"

	"github.com/tinylib/msgp/msgp"
)

type Operator interface {
	AsTuple() []interface{}
}

type OpAdd struct {
	Field    int64
	Argument int64
}

type OpSub struct {
	Field    int64
	Argument int64
}

type OpBitAND struct {
	Field    int64
	Argument uint64
}

type OpBitXOR struct {
	Field    int64
	Argument uint64
}

type OpBitOR struct {
	Field    int64
	Argument uint64
}

type OpDelete struct {
	From  int64
	Count uint64
}

type OpInsert struct {
	Before   int64
	Argument interface{}
}

type OpAssign struct {
	Field    int64
	Argument interface{}
}

type OpSplice struct {
	Field    int64
	Offset   uint64
	Position uint64
	Argument string
}

func (op *OpAdd) AsTuple() []interface{} {
	return []interface{}{"+", op.Field, op.Argument}
}

func (op *OpSub) AsTuple() []interface{} {
	return []interface{}{"-", op.Field, op.Argument}
}

func (op *OpBitAND) AsTuple() []interface{} {
	return []interface{}{"&", op.Field, op.Argument}
}

func (op *OpBitXOR) AsTuple() []interface{} {
	return []interface{}{"^", op.Field, op.Argument}
}

func (op *OpBitOR) AsTuple() []interface{} {
	return []interface{}{"|", op.Field, op.Argument}
}

func (op *OpDelete) AsTuple() []interface{} {
	return []interface{}{"#", op.From, op.Count}
}

func (op *OpInsert) AsTuple() []interface{} {
	return []interface{}{"!", op.Before, op.Argument}
}

func (op *OpAssign) AsTuple() []interface{} {
	return []interface{}{"=", op.Field, op.Argument}
}

func (op *OpSplice) AsTuple() []interface{} {
	return []interface{}{":", op.Field, op.Position, op.Offset, op.Argument}
}

func marshalOperator(op Operator, buf []byte) ([]byte, error) {
	return msgp.AppendIntf(buf, op.AsTuple())
}

func unmarshalOperator(data []byte) (op Operator, buf []byte, err error) {
	buf = data

	var n uint32
	if n, buf, err = msgp.ReadArrayHeaderBytes(buf); err != nil {
		return
	}

	var str string
	if str, buf, err = msgp.ReadStringBytes(buf); err != nil {
		return
	}

	var field0 int64
	if field0, buf, err = msgp.ReadInt64Bytes(buf); err != nil {
		return
	}

	switch str {
	case "+":
		if n != 3 {
			return nil, buf, fmt.Errorf("unexpected number of arguments in OpAdd: %d", n)
		}
		opAdd := &OpAdd{Field: field0}
		if opAdd.Argument, buf, err = msgp.ReadInt64Bytes(buf); err != nil {
			return
		}
		op = opAdd
	case "-":
		if n != 3 {
			return nil, buf, fmt.Errorf("unexpected number of arguments in OpSub: %d", n)
		}
		opSub := &OpSub{Field: field0}
		if opSub.Argument, buf, err = msgp.ReadInt64Bytes(buf); err != nil {
			return
		}
		op = opSub
	case "&":
		if n != 3 {
			return nil, buf, fmt.Errorf("unexpected number of arguments in OpBitAND: %d", n)
		}
		opAnd := &OpBitAND{Field: field0}
		if opAnd.Argument, buf, err = msgp.ReadUint64Bytes(buf); err != nil {
			return
		}
		op = opAnd
	case "^":
		if n != 3 {
			return nil, buf, fmt.Errorf("unexpected number of arguments in OpBitXOR: %d", n)
		}
		opXOR := &OpBitXOR{Field: field0}
		if opXOR.Argument, buf, err = msgp.ReadUint64Bytes(buf); err != nil {
			return
		}
		op = opXOR
	case "|":
		if n != 3 {
			return nil, buf, fmt.Errorf("unexpected number of arguments in OpBitOR: %d", n)
		}
		opOR := &OpBitOR{Field: field0}
		if opOR.Argument, buf, err = msgp.ReadUint64Bytes(buf); err != nil {
			return
		}
		op = opOR
	case "#":
		if n != 3 {
			return nil, buf, fmt.Errorf("unexpected number of arguments in OpDelete: %d", n)
		}
		opDel := &OpDelete{From: field0}
		if opDel.Count, buf, err = msgp.ReadUint64Bytes(buf); err != nil {
			return
		}
		op = opDel
	case "!":
		if n != 3 {
			return nil, buf, fmt.Errorf("unexpected number of arguments in OpInsert: %d", n)
		}
		opIns := &OpInsert{Before: field0}
		if opIns.Argument, buf, err = msgp.ReadIntfBytes(buf); err != nil {
			return
		}
		op = opIns
	case "=":
		if n != 3 {
			return nil, buf, fmt.Errorf("unexpected number of arguments in OpAssign: %d", n)
		}
		opAss := &OpAssign{Field: field0}
		if opAss.Argument, buf, err = msgp.ReadIntfBytes(buf); err != nil {
			return
		}
		op = opAss
	case ":":
		if n != 5 {
			return nil, buf, fmt.Errorf("unexpected number of arguments in OpSplice: %d", n)
		}
		opSpl := &OpSplice{Field: field0}
		if opSpl.Position, buf, err = msgp.ReadUint64Bytes(buf); err != nil {
			return
		}
		if opSpl.Offset, buf, err = msgp.ReadUint64Bytes(buf); err != nil {
			return
		}
		if opSpl.Argument, buf, err = msgp.ReadStringBytes(buf); err != nil {
			return
		}
		op = opSpl
	default:
		return nil, buf, fmt.Errorf("uknown op %s", str)
	}

	return
}
