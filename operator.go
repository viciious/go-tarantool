package tnt

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
	Offset   uint64
	Position uint64
	Argument string
}
