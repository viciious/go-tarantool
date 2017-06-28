package tarantool

import (
	"bufio"
	"io"

	uuid "github.com/satori/go.uuid"
)

const (
	procLUALastSnapLSN = "lastsnaplsn"
)

// PacketIterator is a wrapper around Slave provided iteration over new Packets functionality.
type PacketIterator interface {
	Next() (*Packet, error)
}

// Slave connects to Tarantool 1.6 instance and subscribes for changes.
// Tarantool instance acting as a master sees Slave like any replica in replication set.
// Slave can't be used concurrently, route responses from returned channel instead.
type Slave struct {
	c          *Connection
	cr         *bufio.Reader
	cw         *bufio.Writer
	UUID       string
	VClock     VectorClock
	ReplicaSet struct {
		UUID      string
		Instances ReplicaSet
	}
	next func() (*Packet, error) // next stores current iterator
	p    *Packet                 // p stores last packet for Packet method
	err  error                   // err stores last error for Err method
}

// NewSlave instance of Slave with tarantool master uri
// URI is parsed by url package and therefore should contains
// any scheme supported by net.Dial
func NewSlave(uri string, opts ...Options) (s *Slave, err error) {

	s = new(Slave)
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
	// prevent from NPE in Next method
	s.next = s.nextEOF

	return s, nil
}

func (s *Slave) parseOptions(uri string, options Options) (err error) {

	if len(options.UUID) == 0 {
		s.UUID = uuid.NewV1().String()
	} else {
		s.UUID = options.UUID
	}

	s.ReplicaSet.UUID = options.ReplicaSetUUID

	return nil
}

// Attach Slave to Replica Set and subscribe for DML requests, starting from lsn.
// Use out chan for asynchronous packet receiving or synchronous PacketIterator otherwise.
func (s *Slave) Attach(lsn int64, out ...chan *Packet) (PacketIterator, error) {
	if err := s.Join(); err != nil {
		return nil, err
	}
	return s.Subscribe(lsn, out...)
}

// Close Slave connection to Master
func (s *Slave) Close() error {
	return s.disconnect()
}

// Join the Replica Set using Master instance
func (s *Slave) Join() (err error) {

	it, err := s.JoinWithSnap()
	if err != nil {
		return err
	}

	for {
		_, err = it.Next()
		if err != nil {
			break
		}
	}

	if err == io.EOF {
		return nil
	}

	return
}

// JoinWithSnap the Replica Set using Master instance.
// Snapshot logs is available through the given out channel or returned PacketIterator.
// (In truth, Slave itself is returned in PacketIterator wrapper)
func (s *Slave) JoinWithSnap(out ...chan *Packet) (it PacketIterator, err error) {

	if err = s.join(); err != nil {
		return nil, err
	}

	// set iterator for the Next method
	s.next = s.nextSnap

	if s.isEmptyChan(out...) {
		// no chan means synchronous snapshot scanning
		return s, nil
	}

	respc := out[0]
	defer close(respc)

	for {
		p, err := s.Next()
		if err != nil {
			// TODO: one should close chan out and save p and err
			break
		}
		respc <- p
	}

	if err == io.EOF {
		return nil, nil
	}

	return nil, err
}

// isEmptyChan parses channels option.
func (s *Slave) isEmptyChan(out ...chan *Packet) bool {
	return len(out) == 0 || out[0] == nil
}

// Subscribe for every DML request (insert, update, delete, replace, upsert) from master since lsn.
// Replica Set and self UUID should be set before call subscribe. Use options in New or Join for it.
// Subscribe sends requests asynchronously to out channel specified or use synchronous PacketIterator otherwise.
func (s *Slave) Subscribe(lsn int64, out ...chan *Packet) (it PacketIterator, err error) {
	//don't call subscribe if there are no options had been set or before join request
	if !s.IsInReplicaSet() {
		return nil, ErrNotInReplicaSet
	}

	if err = s.subscribe(lsn); err != nil {
		return nil, err
	}

	// set iterator for the Next method
	s.next = s.nextXlog

	if s.isEmptyChan(out...) {
		// no chan means synchronous dml request receiving
		return s, nil
	}

	// consuming new DML requests asynchronously
	go func(out chan *Packet) {
		defer close(out)
		for {
			p, err := s.Next()
			if err != nil {
				// TODO: one should close chan out and save p and err
				break
			}
			out <- p
		}
	}(out[0])

	return nil, nil
}

// IsInReplicaSet checks whether Slave has Replica Set params or not
func (s *Slave) IsInReplicaSet() bool {
	return len(s.UUID) > 0 && len(s.ReplicaSet.UUID) > 0
}

func (s *Slave) LastSnapLSN() (int64, error) {
	pp, err := s.newPacket(&Call{Name: procLUALastSnapLSN})
	if err != nil {
		return 0, err
	}

	if err = s.send(pp); err != nil {
		return 0, err
	}
	pp.Release()

	if pp, err = s.receive(); err != nil {
		return 0, err
	}
	defer pp.Release()

	p, err := decodePacket(pp)
	if err != nil {
		return 0, err
	}
	if p.code != OKRequest {
		s.p = p
		s.err = p.Result.Error
		return 0, s.err
	}

	res := p.Result.Data
	if len(res) == 0 || len(res[0]) == 0 {
		return 0, ErrBadResult
	}

	lsn, ok := res[0][0].(uint64)
	if !ok {
		return 0, ErrBadResult
	}
	return int64(lsn), nil
}

// join send JOIN request.
func (s *Slave) join() (err error) {
	pp, err := s.newPacket(&Join{UUID: s.UUID})
	if err != nil {
		return
	}

	if err = s.send(pp); err != nil {
		return err
	}
	pp.Release()

	return nil
}

// subscribe sends SUBSCRIBE request and waits for VCLOCK response
func (s *Slave) subscribe(lsn int64) error {

	pp, err := s.newPacket(&Subscribe{
		UUID:           s.UUID,
		ReplicaSetUUID: s.ReplicaSet.UUID,
		LSN:            lsn,
	})
	if err != nil {
		return err
	}

	if err = s.send(pp); err != nil {
		return err
	}
	pp.Release()

	if pp, err = s.receive(); err != nil {
		return err
	}
	defer pp.Release()

	p, err := decodePacket(pp)
	if err != nil {
		return err
	}
	if p.code != OKRequest {
		// TODO: little hack for complex attach
		s.p = p
		return p.Result.Error
	}

	v := new(VClock)
	err = v.UnmarshalBinary(pp.body)
	if err != nil {
		return err
	}

	s.VClock = v.VClock

	return nil
}

// HasNext implements bufio.Scanner Scan style iterator.
func (s *Slave) HasNext() bool {
	s.p, s.err = s.Next()
	if s.err != nil {
		if s.err == io.EOF {
			s.err = nil
		}
		return false
	}
	return true
}

// Packet has been got by HasNext method.
func (s *Slave) Packet() *Packet {
	return s.p
}

// Err has been got by HasNext method.
func (s *Slave) Err() error {
	return s.err
}

// Next implements PacketIterator interface.
func (s *Slave) Next() (*Packet, error) {
	// Next wraps unexported "next" fields.
	// Because of exported Next field can't implements needed interface itself.

	p, err := s.next()
	if err != nil {
		// don't iterate after error has been occurred
		s.next = s.nextEOF
	}
	return p, err
}

// nextXlog iterates new packets (responses on SUBSCRIBE request).
func (s *Slave) nextXlog() (p *Packet, err error) {

	pp, err := s.receive()
	if err != nil {
		return nil, err
	}
	defer pp.Release()

	p, err = decodePacket(pp)
	if err != nil {
		return nil, err
	}

	return p, nil
}

// nextSnap iterates responses on JOIN request.
// At the end it returns io.EOF error and nil packet.
// While iterating all
func (s *Slave) nextSnap() (p *Packet, err error) {

	pp, err := s.receive()
	if err != nil {
		return nil, err
	}
	defer pp.Release()

	p, err = decodePacket(pp)
	if err != nil {
		return nil, err
	}

	// we have to parse snapshot logs to find replica set instances, UUID

	// this response error type means that UUID had been joined Replica Set already
	joined := ErrorFlag | ErrTupleFound

	switch p.code {
	case InsertRequest:
		q := p.Request.(*Insert)
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
	case OKRequest:
		v := new(VClock)
		err = v.UnmarshalBinary(pp.body)
		if err != nil {
			return nil, err
		}
		s.VClock = v.VClock
		return nil, io.EOF
	case joined:
		// already joined
		return nil, io.EOF
	}

	return p, nil
}

// nextEOF is empty iterator to avoid calling others in inappropriate cases
func (s *Slave) nextEOF() (*Packet, error) {
	return nil, io.EOF
}

// connect to tarantool instance (dial + handshake + auth)
func (s *Slave) connect(uri string, options *Options) (err error) {
	dsn, opts, err := parseOptions(uri, options)
	if err != nil {
		return
	}
	conn, err := newConn(dsn.Scheme, dsn.Host, opts)
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
func (s *Slave) disconnect() (err error) {
	s.c.stop()
	return
}

// send packed packet to the connection buffer, flush buffer.
func (s *Slave) send(pp *packedPacket) (err error) {
	if _, err = pp.WriteTo(s.cw); err != nil {
		return
	}
	return s.cw.Flush()
}

// receive new response packet
func (s *Slave) receive() (*packedPacket, error) {
	return readPacked(s.cr)
}

// newPacket compose packet from body
func (s *Slave) newPacket(q Query) (pp *packedPacket, err error) {
	pp = packIproto(0, s.c.nextID())
	pp.code, err = q.Pack(s.c.packData, &pp.buffer)
	if err != nil {
		pp.Release()
		pp = nil
	}
	return
}
