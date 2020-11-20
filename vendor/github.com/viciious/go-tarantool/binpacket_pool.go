package tarantool

type BinaryPacketPool struct {
	queue chan *BinaryPacket
}

func newBinaryPacketPool() *BinaryPacketPool {
	return &BinaryPacketPool{
		queue: make(chan *BinaryPacket, 1024),
	}
}

func (p *BinaryPacketPool) GetWithID(requestID uint64) (pp *BinaryPacket) {
	select {
	case pp = <-p.queue:
	default:
		pp = &BinaryPacket{}
	}

	pp.Reset()
	pp.pool = p
	pp.packet.requestID = requestID
	return
}

func (p *BinaryPacketPool) Get() *BinaryPacket {
	return p.GetWithID(0)
}

func (p *BinaryPacketPool) Put(pp *BinaryPacket) {
	pp.pool = nil
	select {
	case p.queue <- pp:
	default:
	}
}

func (p *BinaryPacketPool) Close() {
	close(p.queue)
}
