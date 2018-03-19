package tarantool

type BinaryPacketPool struct {
	queue chan *BinaryPacket
}

func newPackedPacketPool() *BinaryPacketPool {
	return &BinaryPacketPool{
		queue: make(chan *BinaryPacket, 512),
	}
}

func (p *BinaryPacketPool) GetWithID(requestID uint64) (pp *BinaryPacket) {
	select {
	case pp = <-p.queue:
		pp.Reset()
		pp.pool = p
	default:
		pp = &BinaryPacket{}
		pp.Reset()
	}
	pp.packet.requestID = requestID
	return
}

func (p *BinaryPacketPool) Get() *BinaryPacket {
	return p.GetWithID(0)
}

func (p *BinaryPacketPool) Put(pp *BinaryPacket) {
	pp.pool = nil
	p.queue <- pp
}

func (p *BinaryPacketPool) Close() {
	close(p.queue)
}
