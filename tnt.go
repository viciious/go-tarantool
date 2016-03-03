package tnt

type Query interface {
	Pack(requestID uint32, defaultSpace interface{}, cache *packCache) ([]byte, error)
}

type request struct {
	query     Query
	raw       []byte
	replyChan chan *Response
}
