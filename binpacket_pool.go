package tarantool

type binaryPacketPool struct {
	queue chan *binaryPacket
}

func newPackedPacketPool() *binaryPacketPool {
	return &binaryPacketPool{
		queue: make(chan *binaryPacket, 512),
	}
}

func (p *binaryPacketPool) GetWithID(requestID uint64) (pp *binaryPacket) {
	select {
	case pp = <-p.queue:
		pp.Reset()
		pp.pool = p
	default:
		pp = &binaryPacket{}
		pp.Reset()
	}
	pp.packet.requestID = requestID
	return
}

func (p *binaryPacketPool) Get() *binaryPacket {
	return p.GetWithID(0)
}

func (p *binaryPacketPool) Put(pp *binaryPacket) {
	pp.pool = nil
	p.queue <- pp
}

func (p *binaryPacketPool) Close() {
	close(p.queue)
}
