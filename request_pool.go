package tarantool

type cappedRequestPool struct {
	queue chan *request
	reuse bool
}

func newCappedRequestPool() *cappedRequestPool {
	return &cappedRequestPool{
		queue: make(chan *request, 1024),
		reuse: false,
	}
}

func (p *cappedRequestPool) Get() (r *request) {
	if !p.reuse {
		return &request{}
	}

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
	if !p.reuse {
		return
	}

	if r == nil {
		return
	}

	select {
	case p.queue <- r:
	default:
	}
}

func (p *cappedRequestPool) Close() {
	close(p.queue)
}
