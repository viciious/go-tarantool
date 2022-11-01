package tarantool

import (
	"io"
)

// AnonSlave connects to Tarantool >= 2.3.1 instance and subscribes for changes as anonymous replica.
// Tarantool instance acting as a master sees AnonSlave like anonymous replica.
// AnonSlave can't be used concurrently, route responses from returned channel instead.
type AnonSlave struct {
	Slave
}

// NewAnonSlave returns new AnonSlave instance.
// URI is parsed by url package and therefore should contains any scheme supported by net.Dial.
func NewAnonSlave(uri string, opts ...Options) (as *AnonSlave, err error) {
	s, err := NewSlave(uri, opts...)
	if err != nil {
		return nil, err
	}

	// check tarantool version. Anonymous replica support was added in Tarantool 2.3.1
	if s.Version() < version2_3_1 {
		return nil, ErrOldVersionAnon
	}

	return &AnonSlave{*s}, nil
}

// JoinWithSnap fetch snapshots from Master instance.
// Snapshot logs is available through the given out channel or returned PacketIterator.
// (In truth, Slave itself is returned in PacketIterator wrapper)
func (s *AnonSlave) JoinWithSnap(out ...chan *Packet) (it PacketIterator, err error) {
	if err = s.fetchSnapshot(); err != nil {
		return nil, err
	}

	// set iterator for the Next method
	s.next = s.nextSnap

	if s.isEmptyChan(out...) {
		// no chan means synchronous snapshot scanning
		return s, nil
	}

	defer close(out[0])
	for s.HasNext() {
		out[0] <- s.Packet()
	}

	return nil, s.Err()
}

// Join fetches snapshots using Master instance.
func (s *AnonSlave) Join() (err error) {
	_, err = s.JoinWithSnap()
	if err != nil {
		return err
	}

	for s.HasNext() {
	}

	return s.Err()
}

// Subscribe for DML requests (insert, update, delete, replace, upsert) since vector clock.
// Variadic lsn is start vector clock. Each lsn is one clock in vector (sequentially).
// One lsn is enough for master-slave replica set.
// Subscribe sends requests asynchronously to out channel specified or use synchronous PacketIterator otherwise.
// For anonymous replica it is not necessary to call Join or JoinWithSnap before Subscribe.
func (s *AnonSlave) Subscribe(lsns ...uint64) (it PacketIterator, err error) {
	if len(lsns) == 0 || len(lsns) >= VClockMax {
		return nil, ErrVectorClock
	}

	if err = s.subscribe(lsns...); err != nil {
		return nil, err
	}

	// set iterator for the Next method
	s.next = s.nextXlog

	// Start sending heartbeat messages to master
	go s.heartbeat()

	return s, nil
}

// Attach AnonSlave to Replica Set as an anonymous and subscribe for the new(!) DML requests.
// Attach calls Join and then Subscribe with VClock = s.VClock[1:]...
// If didn't call Join before Attach then you need to set VClock first either manually or using JoinWithSnap.
// Use out chan for asynchronous packet receiving or synchronous PacketIterator otherwise.
// If you need all requests in chan use JoinWithSnap(chan) and then s.Subscribe(s.VClock[1:]...).
func (s *AnonSlave) Attach(out ...chan *Packet) (it PacketIterator, err error) {
	if err = s.Join(); err != nil {
		return nil, err
	}

	// skip reserved zero index of the Vector Clock
	if len(s.VClock) <= 1 {
		return nil, ErrVectorClock
	}

	if it, err = s.Subscribe(s.VClock[1:]...); err != nil {
		return nil, err
	}

	// no chan means synchronous dml request receiving
	if s.isEmptyChan(out...) {
		return it, nil
	}

	// consume new DML requests and send them to the given chan
	go func(out chan *Packet) {
		defer close(out)
		for s.HasNext() {
			out <- s.Packet()
		}
	}(out[0])

	// return nil iterator to avoid concurrent using of the Next method
	return nil, nil
}

func (s *AnonSlave) fetchSnapshot() (err error) {
	pp, err := s.newPacket(&FetchSnapshot{})
	if err != nil {
		return
	}

	if err = s.send(pp); err != nil {
		return err
	}
	s.c.releasePacket(pp)

	if pp, err = s.receive(); err != nil {
		return err
	}
	defer pp.Release()

	p := &Packet{}
	if err := p.UnmarshalBinary(pp.body); err != nil {
		return err
	}

	if p.Cmd != OKCommand {
		s.p = p
		if p.Result == nil {
			return ErrBadResult
		}
		return p.Result.Error
	}

	v := new(VClock)
	_, err = v.UnmarshalMsg(pp.body)
	if err != nil {
		return err
	}

	s.VClock = v.VClock

	return nil
}

// subscribe sends SUBSCRIBE request and waits for VCLOCK response.
func (s *AnonSlave) subscribe(lsns ...uint64) error {
	vc := NewVectorClock(lsns...)
	pp, err := s.newPacket(&Subscribe{
		UUID:           s.UUID,
		ReplicaSetUUID: s.ReplicaSet.UUID,
		VClock:         vc,
		Anon:           true,
	})
	if err != nil {
		return err
	}

	if err = s.send(pp); err != nil {
		return err
	}
	s.c.releasePacket(pp)

	if pp, err = s.receive(); err != nil {
		return err
	}
	defer s.c.releasePacket(pp)

	p := &pp.packet
	err = p.UnmarshalBinary(pp.body)
	if err != nil {
		return err
	}

	sub := new(SubscribeResponse)
	_, err = sub.UnmarshalMsg(pp.body)
	if err != nil {
		return err
	}

	// validate the response replica set UUID only if it is not empty
	if s.ReplicaSet.UUID != "" && sub.ReplicaSetUUID != "" && s.ReplicaSet.UUID != sub.ReplicaSetUUID {
		return NewUnexpectedReplicaSetUUIDError(s.ReplicaSet.UUID, sub.ReplicaSetUUID)
	}

	if sub.ReplicaSetUUID != "" {
		s.ReplicaSet.UUID = sub.ReplicaSetUUID
	}
	s.VClock = sub.VClock

	return nil
}

// nextSnap iterates responses on JOIN request.
// At the end it returns io.EOF error and nil packet.
// While iterating all
func (s *AnonSlave) nextSnap() (p *Packet, err error) {
	pp, err := s.receive()
	if err != nil {
		return nil, err
	}
	defer s.c.releasePacket(pp)

	p = &Packet{}
	err = p.UnmarshalBinary(pp.body)
	if err != nil {
		return nil, err
	}

	// we have to parse snapshot logs to find replica set instances, UUID
	switch p.Cmd {
	case InsertCommand:
		q := p.Request.(*Insert)
		if q.Space == SpaceSchema {
			key := q.Tuple[0].(string)
			if key == SchemaKeyClusterUUID {
				if s.ReplicaSet.UUID != "" && s.ReplicaSet.UUID != q.Tuple[1].(string) {
					return nil, NewUnexpectedReplicaSetUUIDError(s.ReplicaSet.UUID, q.Tuple[1].(string))
				}
				s.ReplicaSet.UUID = q.Tuple[1].(string)
			}
		}
	case OKCommand:
		v := new(VClock)
		_, err = v.UnmarshalMsg(pp.body)
		if err != nil {
			return nil, err
		}
		// ignore this VClock for anon replica

		return nil, io.EOF
	}

	return p, nil
}
