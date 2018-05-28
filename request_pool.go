package tarantool

type cappedRequestPool struct {
	queue chan *request
}

func newCappedRequestPool() *cappedRequestPool {
	return &cappedRequestPool{
		queue: make(chan *request, 1024),
	}
}

func (p *cappedRequestPool) Get() (r *request) {
	select {
	case r = <-p.queue:
		r.opaque = nil
		r.replyChan = nil
	default:
		r = &request{}
	}
	return
}

func (p *cappedRequestPool) Put(r *request) {
	select {
	case p.queue <- r:
	default:
	}
}

func (p *cappedRequestPool) Close() {
	close(p.queue)
}
