package tarantool

type binaryPacketPool struct {
	queue chan *binaryPacket
}

func newPackedPacketPool() *binaryPacketPool {
	return &binaryPacketPool{
		queue: make(chan *binaryPacket, 512),
	}
}

func (p *binaryPacketPool) Get() *binaryPacket {
	select {
	case pp := <-p.queue:
		pp.pool = p
		pp.code = 0
		pp.requestID = 0
		pp.body = pp.body[:0]
		return pp
	default:
		pp := &binaryPacket{}
		return pp
	}
}

func (p *binaryPacketPool) Put(pp *binaryPacket) {
	pp.pool = nil
	p.queue <- pp
}

func (p *binaryPacketPool) Close() {
	close(p.queue)
}
