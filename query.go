package tarantool

type Query interface {
	GetCommandID() uint
}

type internalQuery interface {
	packMsg(data *packData, b []byte) ([]byte, error)
}

func NewQuery(cmd uint) Query {
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
	case Call17Command:
		return &Call17{}
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
