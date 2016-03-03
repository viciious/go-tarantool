package tnt

type Query interface {
	Pack(requestID uint32, defaultSpace string) ([]byte, error)
}

type request struct {
	query     Query
	raw       []byte
	replyChan chan *Response
}
