package tarantool

type Iterator struct {
	Iter uint8
}

func (it Iterator) String() string {
	switch it.Iter {
	case IterEq:
		return "EQ"
	case IterReq:
		return "REQ"
	case IterAll:
		return "ALL"
	case IterLt:
		return "LT"
	case IterLe:
		return "LE"
	case IterGe:
		return "GE"
	case IterGt:
		return "GT"
	case IterBitsAllSet:
		return "BITS_ALL_SET"
	case IterBitsAnySet:
		return "BITS_ANY_SET"
	case IterBitsAllNotSet:
		return "BITS_ALL_NOT_SET"
	}
	return "ER"
}
