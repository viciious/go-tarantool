package tarantool

type Operator interface {
	AsTuple() []interface{}
}

type OpAdd struct {
	Field    uint64
	Argument int64
}

type OpSub struct {
	Field    uint64
	Argument int64
}

type OpBitAND struct {
	Field    uint64
	Argument uint64
}

type OpBitXOR struct {
	Field    uint64
	Argument uint64
}

type OpBitOR struct {
	Field    uint64
	Argument uint64
}

type OpDelete struct {
	From  uint64
	Count uint64
}

type OpInsert struct {
	Before   uint64
	Argument interface{}
}

type OpAssign struct {
	Field    uint64
	Argument interface{}
}

type OpSplice struct {
	Field    uint64
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
