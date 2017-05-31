package tarantool

type packedPacketPool struct {
	queue chan *packedPacket
}

func newPackedPacketPool() *packedPacketPool {
	return &packedPacketPool{
		queue: make(chan *packedPacket, 512),
	}
}

func (p *packedPacketPool) Get() *packedPacket {
	select {
	case pp := <-p.queue:
		pp.pool = p
		pp.code = 0
		pp.requestID = 0
		pp.body = nil
		pp.buffer.Reset()
		return pp
	default:
		pp := &packedPacket{}
		return pp
	}
}

func (p *packedPacketPool) Put(pp *packedPacket) {
	pp.pool = nil
	p.queue <- pp
}

func (p *packedPacketPool) Close() {
	close(p.queue)
}
