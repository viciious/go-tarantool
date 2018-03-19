package tarantool

type Ping struct {
}

var _ Query = (*Ping)(nil)

func (q Ping) GetCommandID() int {
	return PingCommand
}

func (q Ping) PackMsg(data *packData, b []byte) (o []byte, err error) {
	return b, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (q *Ping) UnmarshalBinary(data []byte) (err error) {
	_, err = q.UnmarshalMsg(data)
	return err
}

// UnmarshalMsg implements msgp.Unmarshaller
func (q *Ping) UnmarshalMsg([]byte) (buf []byte, err error) {
	return buf, nil
}
