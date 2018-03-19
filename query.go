package tarantool

type Query interface {
	GetCommandID() int
	UnmarshalBinary(data []byte) error
	UnmarshalMsg(data []byte) ([]byte, error)
	PackMsg(data *packData, b []byte) ([]byte, error)
}

func NewQuery(cmd int) Query {
	switch cmd {
	case SelectCommand:
		return &Select{}
	case AuthCommand:
		return &Auth{}
	case InsertCommand:
		return &Insert{}
	case ReplaceCommand:
		return &Replace{}
	case DeleteCommand:
		return &Delete{}
	case CallCommand:
		return &Call{}
	case UpdateCommand:
		return &Update{}
	case UpsertCommand:
		return &Upsert{}
	case PingCommand:
		return &Ping{}
	case EvalCommand:
		return &Eval{}
	default:
		return nil
	}
}
