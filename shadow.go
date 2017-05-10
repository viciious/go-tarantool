package tarantool

import (
	"bufio"
	"bytes"

	uuid "github.com/satori/go.uuid"
)

// Shadow connects to Tarantool 1.6 instance and subscribes for changes.
// Tarantool instance acting as a master sees Shadow like any replica in replication set.
// Shadow can't be used concurrently, route responses from returned channel instead.
type Shadow struct {
	c          *Connection
	cr         *bufio.Reader
	cw         *bufio.Writer
	UUID       string
	VClock     VectorClock
	ReplicaSet struct {
		UUID      string
		Instances ReplicaSet
	}
}

// NewShadow instance of Shadow with tarantool master uri
// URI is parsed by url package and therefore should contains
// any scheme supported by net.Dial
func NewShadow(uri string, opts ...Options) (s *Shadow, err error) {

	s = new(Shadow)
	options := Options{}
	if len(opts) > 0 {
		options = opts[0]
	}

	if err = s.parseOptions(uri, options); err != nil {
		return nil, err
	}

	s.ReplicaSet.Instances = make(ReplicaSet, ReplicaSetMaxSize)

	// it is discussable to connect to instance in instance creation
	if err = s.connect(uri, &options); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Shadow) parseOptions(uri string, options Options) (err error) {

	if len(options.UUID) == 0 {
		s.UUID = uuid.NewV1().String()
	} else {
		s.UUID = options.UUID
	}

	s.ReplicaSet.UUID = options.ReplicaSetUUID

	return nil
}

// Attach Shadow to Replica Set and subscribe for DML requests, starting from lsn.
// Join replica set, receive snapshot (inserts requests), subscribe for xlogs (all dml requests)
func (s *Shadow) Attach(lsn int64, out ...chan *Packet) (<-chan *Packet, error) {
	if err := s.Join(); err != nil {
		return nil, err
	}
	return s.consume(lsn, out...)
}

// Detach Shadow from Master
func (s *Shadow) Detach() error {
	return s.disconnect()
}

// Join the Replica Set using Master instance
func (s *Shadow) Join(out ...chan *Packet) (err error) {
	var respc chan *Packet
	if len(out) > 0 && out[0] != nil {
		respc = out[0]
	} else {
		// empty chan cause it is useless
		respc = make(chan *Packet, 1)
		go func(ch chan *Packet) {
			for range ch {
			}
		}(respc)
	}

	err = s.join(respc)
	close(respc)
	return
}

// Subscribe for every DML request (insert, update, delete, replace, upsert) from master
// Replica Set and self params (UUID, IDs) should be set before call subscribe.
// Use options in New or Join before.
func (s *Shadow) Subscribe(lsn int64, out ...chan *Packet) (r <-chan *Packet, err error) {
	//don't call subscribe if there are no options had been set or before join request
	if !s.IsInReplicaSet() {
		return nil, ErrNotInReplicaSet
	}

	return s.consume(lsn, out...)
}

// IsInReplicaSet checks whether Shadow has Replica Set params or not
func (s *Shadow) IsInReplicaSet() bool {
	return len(s.UUID) > 0 && len(s.ReplicaSet.UUID) > 0
}

// join send JOIN request and parse responses till OK/Error response will be received
func (s *Shadow) join(out chan<- *Packet) (err error) {

	pp, err := s.newPacket(&Join{UUID: s.UUID})
	if err != nil {
		return
	}
	defer func() {
		if pp != nil {
			pp.Release()
		}
	}()

	if err = s.send(pp); err != nil {
		return err
	}
	pp.Release()

	var p *Packet
	// this response error type means that UUID had been joined Replica Set already
	joined := ErrorFlag + ErrTupleFound
	for {
		pp, err = s.receive()
		if err != nil {
			return err
		}

		p, err = decodePacket(pp)
		if err != nil {
			return err
		}

		// we have to parse snapshot logs to find replica set instances, UUID,

		switch uint32(p.code) {
		case uint32(InsertRequest):
			q := p.request.(*Insert)
			switch q.Space {
			case SpaceSchema:
				// assert space _schema always has str index on field one
				// and in "cluster" tuple uuid is string too
				// {"cluster", "ea74fc91-54fe-4f64-adae-ad2bc3eb4194"}
				key := q.Tuple[0].(string)
				if key == SchemaKeyClusterUUID {
					s.ReplicaSet.UUID = q.Tuple[1].(string)
				}

			case SpaceCluster:
				// fill in Replica Set from _cluster space; format:
				// {0x1, "89b1203b-acda-4ff1-ae76-8069145344b8"}
				// {0x2, "7c025e42-2394-11e7-aacf-0242ac110002"}

				// in reality _cluster key field is decoded to uint64
				// but we know exactly that it can be casted to uint8 without loosing data
				instanceID := uint32(q.Tuple[0].(uint64))
				// uuid
				s.ReplicaSet.Instances[instanceID] = q.Tuple[1].(string)
			}
		// TODO: change name from BadRequest to OKRequest
		case uint32(BadRequest):
			q := new(VClock)
			r := bytes.NewReader(pp.body)
			q.Unpack(r)
			s.VClock = q.VClock
			return nil
		case uint32(joined):
			// already joined
			return nil
		}
		pp.Release()
		out <- p
	}
}

// subscribe sends SUBSCRIBE request and waits for VCLOCK response
func (s *Shadow) subscribe(lsn int64) (err error) {

	pp, err := s.newPacket(&Subscribe{
		UUID:           s.UUID,
		ReplicaSetUUID: s.ReplicaSet.UUID,
		LSN:            lsn,
	})
	if err != nil {
		return
	}
	defer func() {
		if pp != nil {
			pp.Release()
		}
	}()

	if err = s.send(pp); err != nil {
		return err
	}
	pp.Release()

	pp, err = s.receive()
	if err != nil {
		return
	}

	p, err := decodePacket(pp)
	if err != nil {
		return err
	}
	if uint32(p.code) == uint32(BadRequest) {
		q := new(VClock)
		r := bytes.NewReader(pp.body)
		q.Unpack(r)
		s.VClock = q.VClock
		return nil
	}

	return p.result.Error
}

// consume makes subscribe procedure and launch consumer worker with
// provided out channel or with made one.
func (s *Shadow) consume(lsn int64, out ...chan *Packet) (r <-chan *Packet, err error) {

	var respc chan *Packet
	if len(out) > 0 && out[0] != nil {
		respc = out[0]
	} else {
		respc = make(chan *Packet, 1)
	}

	if err = s.subscribe(lsn); err != nil {
		return
	}

	// start consuming new DML requests
	go s.consumer(respc)

	return respc, nil
}

// consumer is a worker that receive responses from tarantool instance infinitely.
// Close (s.Detach) connection to stop consuming.
// There is no "stop subscribing" command in protocol anyway.
func (s *Shadow) consumer(out chan<- *Packet) {
	var p *Packet
	var pp *packedPacket
	var err error

	defer func() {
		if pp != nil {
			pp.Release()
		}
	}()

	defer close(out)

	for {
		pp, err = s.receive()
		if err != nil {
			return
		}

		p, err = decodePacket(pp)
		if err != nil {
			return
		}

		out <- p

		pp.Release()
	}
}

// connect to tarantool instance (dial + handshake + auth)
func (s *Shadow) connect(uri string, opts *Options) (err error) {
	conn, err := newConn(uri, opts)
	if err != nil {
		return
	}
	s.c = conn
	s.cr = bufio.NewReaderSize(s.c.tcpConn, DefaultReaderBufSize)
	// for better error checking while writing to connection
	s.cw = bufio.NewWriter(s.c.tcpConn)
	return
}

// disconnect call stop on shadow connection instance
func (s *Shadow) disconnect() (err error) {
	s.c.stop()
	return
}

// send packed packet to the connection buffer, flush buffer and release packet
func (s *Shadow) send(pp *packedPacket) (err error) {
	if _, err = pp.WriteTo(s.cw); err != nil {
		return
	}
	return s.cw.Flush()
}

// receive new response packet
func (s *Shadow) receive() (*packedPacket, error) {
	return readPacked(s.cr)
}

// newPacket compose packet from body
func (s *Shadow) newPacket(q Query) (pp *packedPacket, err error) {
	pp = packIproto(0, s.c.nextID())
	pp.code, err = q.Pack(s.c.packData, &pp.buffer)
	if err != nil {
		pp.Release()
		pp = nil
	}
	return
}
