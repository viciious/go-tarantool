package tarantool

import (
	"bufio"
	"bytes"

	"io"

	uuid "github.com/satori/go.uuid"
)

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
	p   *Packet // p is a Packet which has just been read.
	err error   // err is an error which has just been gotten.
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
// Use out chan for asynchronous packet receiving or synchronous Consume method otherwise.
func (s *Slave) Attach(lsn int64, out ...chan *Packet) error {
	if err := s.Join(); err != nil {
		return err
	}
	return s.Subscribe(lsn, out...)
}

// Close Slave connection to Master
func (s *Slave) Close() error {
	return s.disconnect()
}

// Join the Replica Set using Master instance
func (s *Slave) Join() (err error) {

	if _, err = s.JoinWithSnap(); err != nil {
		return err
	}

	for s.NextSnap() {
	}

	return s.Err()
}

// JoinWithSnap the Replica Set using Master instance.
// Snapshot logs is available through the given out channel or
// returned snapshot log iterator - Snapshoter. Use NextSnap, Packet, Err methods of the latter.
// (In truth, Slave itself is returned in Snapshoter wrapper)
func (s *Slave) JoinWithSnap(out ...chan *Packet) (it Snapshoter, err error) {

	if err = s.join(); err != nil {
		return nil, err
	}

	// reset internal error
	s.setErr(nil)

	if s.isEmptyChan(out...) {
		// no chan means synchronous snapshot scanning
		return s, nil
	}

	respc := out[0]
	defer close(respc)

	for s.NextSnap() {
		respc <- s.Packet()
	}

	return nil, s.Err()
}

func (s *Slave) isEmptyChan(out ...chan *Packet) bool {
	return len(out) == 0 || out[0] == nil
}

// Subscribe for every DML request (insert, update, delete, replace, upsert) from master since lsn.
// Replica Set and self UUID should be set before call subscribe. Use options in New or Join for it.
// Subscribe sends requests asynchronously to out channel specified or
// use synchronous Consume method otherwise.
func (s *Slave) Subscribe(lsn int64, out ...chan *Packet) (err error) {
	//don't call subscribe if there are no options had been set or before join request
	if !s.IsInReplicaSet() {
		return ErrNotInReplicaSet
	}

	if err = s.subscribe(lsn); err != nil {
		return err
	}

	// reset internal error
	s.setErr(nil)

	if s.isEmptyChan(out...) {
		// no chan means synchronous dml request receiving
		return nil
	}

	// consuming new DML requests asynchronously
	go func(out chan *Packet) {
		defer close(out)
		for s.Consume() {
			out <- s.Packet()
		}
	}(out[0])

	return nil
}

// IsInReplicaSet checks whether Slave has Replica Set params or not
func (s *Slave) IsInReplicaSet() bool {
	return len(s.UUID) > 0 && len(s.ReplicaSet.UUID) > 0
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
	defer func() {
		if pp != nil {
			pp.Release()
		}
	}()

	p, err := decodePacket(pp)
	if err != nil {
		return err
	}
	if p.code != OKRequest {
		return p.result.Error
	}

	q := new(VClock)
	r := bytes.NewReader(pp.body)
	q.Unpack(r)
	s.VClock = q.VClock

	return nil
}

// Consume new packets (responses on SUBSCRIBE request), which then be available through the Packet method.
// It returns false when the scan stops, by reaching an error.
// After Scan returns false, the Err method will return any error that occurred during scanning.
func (s *Slave) Consume() bool {

	pp, err := s.receive()
	if err != nil {
		s.setErr(err)
		return false
	}

	s.p, err = decodePacket(pp)
	if err != nil {
		s.setErr(err)
		return false
	}
	pp.Release()

	return true
}

// NextSnap parses response on JOIN request, which then be available through the Packet method.
// It returns false when the scan stops, either by reaching the end of the input or an error.
// After Scan returns false, the Err method will return any error that occurred during scanning,
// except that if it was io.EOF, Err will return nil.
func (s *Slave) NextSnap() bool {

	pp, err := s.receive()
	if err != nil {
		s.setErr(err)
		return false
	}

	s.p, err = decodePacket(pp)
	if err != nil {
		s.setErr(err)
		return false
	}

	// we have to parse snapshot logs to find replica set instances, UUID,

	// this response error type means that UUID had been joined Replica Set already
	joined := ErrorFlag | ErrTupleFound

	switch s.p.code {
	case InsertRequest:
		q := s.p.Request.(*Insert)
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
		q := new(VClock)
		r := bytes.NewReader(pp.body)
		q.Unpack(r)
		s.VClock = q.VClock
		s.setErr(io.EOF)
		return false
	case joined:
		// already joined
		s.setErr(io.EOF)
		return false
	}
	pp.Release()

	return true
}

// Packet returns last received packet.
func (s *Slave) Packet() *Packet {
	return s.p
}

// Err returns first occured error while iterating snapshot or consuming requests.
func (s *Slave) Err() error {
	if s.err == io.EOF {
		return nil
	}
	return s.err
}

// setErr sets internal error which are published by Err method.
func (s *Slave) setErr(err error) {
	s.err = err
}

// connect to tarantool instance (dial + handshake + auth)
func (s *Slave) connect(uri string, opts *Options) (err error) {
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
func (s *Slave) disconnect() (err error) {
	s.c.stop()
	return
}

// send packed packet to the connection buffer, flush buffer and release packet
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

// Snapshoter is a set of methods to iterate over received snapshot's requests.
type Snapshoter interface {
	NextSnap() bool
	Packet() *Packet
	Err() error
}
