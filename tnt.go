package tnt

type Query interface {
	Pack(requestID uint32, data *packData) ([]byte, error)
}

type request struct {
	query     Query
	raw       []byte
	replyChan chan *Response
}

type hasSpace interface {
	getSpace() interface{}
	setSpace(interface{})
}

type hasIndex interface {
	getIndex() interface{}
	setIndex(interface{})
}
