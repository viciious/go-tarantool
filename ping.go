package tarantool

type Ping struct {
}

var _ Query = (*Ping)(nil)

func (q *Ping) GetCommandID() uint {
	return PingCommand
}

// MarshalMsg implements msgp.Marshaler
func (q *Ping) MarshalMsg(b []byte) ([]byte, error) {
	return b, nil
}

// UnmarshalMsg implements msgp.Unmarshaler
func (q *Ping) UnmarshalMsg([]byte) (buf []byte, err error) {
	return buf, nil
}
