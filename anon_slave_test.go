package tarantool

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"
)

var (
	nullUUID                  = "00000000-0000-0000-0000-000000000000"
	getAnonReplicasExpression = "return box.info.replication_anon"
)

func TestAnonSlaveConnect(t *testing.T) {
	require := require.New(t)
	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	skipUnsupportedVersion(t, box)

	// setup
	opts := Options{User: tnt16User, Password: tnt16Pass}
	s, err := NewAnonSlave(box.Listen, opts)
	require.NoError(err)

	// check
	err = s.connect(box.Listen, opts)
	require.NoError(err)
	s.c.stop()
}

func TestAnonSlaveJoinExpectedReplicaSetUUID(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	skipUnsupportedVersion(t, box)

	// setup
	s, _ := NewAnonSlave(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
		UUID:     tnt16UUID})

	err = s.Join()
	require.NoError(err)

	ReplicaSetUUID := s.ReplicaSet.UUID

	err = s.Close()
	require.NoError(err)

	s, _ = NewAnonSlave(box.Listen, Options{
		User:           tnt16User,
		Password:       tnt16Pass,
		UUID:           tnt16UUID,
		ReplicaSetUUID: ReplicaSetUUID})
	defer s.Close()

	err = s.Join()
	require.NoError(err)

	assert.EqualValues(ReplicaSetUUID, s.ReplicaSet.UUID)
}

func TestAnonSlaveJoinExpectedReplicaSetUUIDFail(t *testing.T) {
	require := require.New(t)

	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	skipUnsupportedVersion(t, box)

	s, _ := NewAnonSlave(box.Listen, Options{
		User:           tnt16User,
		Password:       tnt16Pass,
		UUID:           tnt16UUID,
		ReplicaSetUUID: nullUUID})
	defer s.Close()

	err = s.Join()
	require.Error(err)

	expectedErr := &UnexpectedReplicaSetUUIDError{}
	require.ErrorIsf(err, expectedErr, "Expect errors type: %T, Got: %T", expectedErr, err)
}

func TestAnonSlaveSubscribeExpectedReplicaSetUUID(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	box, err := newTntBoxWithEval()
	require.NoError(err)
	defer box.Close()

	skipUnsupportedVersion(t, box)

	// setup
	s, _ := NewAnonSlave(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
		UUID:     tnt16UUID})

	err = s.Join()
	require.NoError(err)

	ReplicaSetUUID := s.ReplicaSet.UUID

	err = s.Close()
	require.NoError(err)

	s, _ = NewAnonSlave(box.Listen, Options{
		User:           tnt16User,
		Password:       tnt16Pass,
		UUID:           tnt16UUID,
		ReplicaSetUUID: ReplicaSetUUID})
	defer s.Close()

	_, err = s.Subscribe(0)
	require.NoError(err)

	expectedAnonReplicaCount := 1

	assert.EqualValues(ReplicaSetUUID, s.ReplicaSet.UUID)

	// return if tarantool version doesn't have box.info.replication_anon
	if s.Version() < version2_5_1 {
		return
	}

	anonReplicaCount, err := getAnonReplicasCount(box.Listen)
	require.NoError(err)

	require.EqualValues(expectedAnonReplicaCount, anonReplicaCount)
}

func TestAnonSlaveSubscribeExpectedReplicaSetUUIDFail(t *testing.T) {
	require := require.New(t)

	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	skipUnsupportedVersion(t, box)

	s, _ := NewAnonSlave(box.Listen, Options{
		User:           tnt16User,
		Password:       tnt16Pass,
		UUID:           tnt16UUID,
		ReplicaSetUUID: nullUUID})
	defer s.Close()

	_, err = s.Subscribe(0)
	require.Error(err)

	expectedErr := &UnexpectedReplicaSetUUIDError{}
	require.ErrorIsf(err, expectedErr, "Expect errors type: %T, Got: %T", expectedErr, err)
}

func TestAnonSlaveJoinWithSnapSync(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	box, err := newTntBoxWithEval()
	require.NoError(err)
	defer box.Close()

	skipUnsupportedVersion(t, box)

	// setup
	s, _ := NewAnonSlave(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
		UUID:     tnt16UUID,
	})
	defer s.Close()

	expected := struct {
		UUID             string
		AnonReplicaCount int
	}{tnt16UUID, 0} // Join doesn't add anon replica to anon replica list but Attach and Subscribe do

	it, err := s.JoinWithSnap()
	require.NoError(err)

	resultChan := make(chan bool, 1)
	go func(it PacketIterator, rch chan bool) {
		var p *Packet
		var err error
		for {
			p, err = it.Next()
			if err == io.EOF {
				break
			}
			if err != nil || p == nil {
				rch <- false
				return
			}
		}
		// after io.EOF p should be nil
		rch <- p == nil
	}(it, resultChan)

	// check
	timeout := time.After(10 * time.Second)
	select {
	case success := <-resultChan:
		require.True(success, "There is nil packet or error has been happened")
	case <-timeout:
		t.Fatal("Timeout: there is no necessary snaplogs.")
	}
	assert.NotZero(s.ReplicaSet.UUID)

	// return if tarantool version doesn't have box.info.replication_anon
	if s.Version() < version2_5_1 {
		return
	}

	anonReplicaCount, err := getAnonReplicasCount(box.Listen)
	require.NoError(err)

	require.EqualValues(expected.AnonReplicaCount, anonReplicaCount)
}

func TestAnonSlaveHasNextOnJoin(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	box, err := newTntBoxWithEval()
	require.NoError(err)
	defer box.Close()

	skipUnsupportedVersion(t, box)

	// setup
	s, _ := NewAnonSlave(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
		UUID:     tnt16UUID})
	defer s.Close()

	expected := struct {
		UUID             string
		AnonReplicaCount int
	}{tnt16UUID, 0} // Join doesn't add anon replica to anon replica list but Attach and Subscribe do

	_, err = s.JoinWithSnap()
	require.NoError(err)

	resultChan := make(chan bool, 1)
	go func(s *AnonSlave, rch chan bool) {
		for s.HasNext() {
			if s.Err() != nil || s.Packet() == nil {
				rch <- false
				return
			}
		}
		// after io.EOF p should be nil
		rch <- s.Err() == nil && s.Packet() == nil
	}(s, resultChan)

	// check
	timeout := time.After(10 * time.Second)
	select {
	case success := <-resultChan:
		require.True(success, "There is nil packet or error has been happened")
	case <-timeout:
		t.Fatal("Timeout: there is no necessary snaplogs.")
	}
	assert.NotZero(s.ReplicaSet.UUID)

	// return if tarantool version doesn't have box.info.replication_anon
	if s.Version() < version2_5_1 {
		return
	}

	anonReplicaCount, err := getAnonReplicasCount(box.Listen)
	require.NoError(err)

	require.EqualValues(expected.AnonReplicaCount, anonReplicaCount)
}

func TestAnonSlaveIsEmptyChan(t *testing.T) {
	s := &AnonSlave{}

	ch := make(chan *Packet)
	assert.True(t, s.isEmptyChan(), "case empty params")
	assert.False(t, s.isEmptyChan(ch), "case one param")

	tt := []struct {
		in       []chan *Packet
		expected bool
	}{
		{nil, true},
		{[]chan *Packet{}, true},
		{[]chan *Packet{nil}, true},
		{[]chan *Packet{nil, ch}, true},
		{[]chan *Packet{ch}, false},
		{[]chan *Packet{ch, nil}, false},
		{[]chan *Packet{ch, ch}, false},
	}
	for tc, item := range tt {
		actual := s.isEmptyChan(item.in...)
		assert.EqualValues(t, item.expected, actual, "case %v", tc+1)
	}
}

func TestAnonSlaveJoinWithSnapAsync(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	box, err := newTntBoxWithEval()
	require.NoError(err)
	defer box.Close()

	skipUnsupportedVersion(t, box)

	// setup
	s, _ := NewAnonSlave(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
		UUID:     tnt16UUID})
	defer s.Close()

	expected := struct {
		UUID             string
		AnonReplicaCount int
	}{tnt16UUID, 0} // Join doesn't add anon replica to anon replica list but Attach and Subscribe do

	respc := make(chan *Packet, 1)

	var it PacketIterator
	go func() {
		it, err = s.JoinWithSnap(respc)
	}()

	// drain channel and fatal on timeout
	timeout := time.After(10 * time.Second)
loop:
	for {
		select {
		case p, open := <-respc:
			if !open {
				break loop
			}
			require.NotNil(p, "There is nil packet has been received.")
		case <-timeout:
			t.Fatal("Timeout: there is no necessary xlog.")
		}
	}

	// check
	assert.Nil(it)
	assert.NoError(err)
	assert.NotZero(s.ReplicaSet.UUID)

	// return if tarantool version doesn't have box.info.replication_anon
	if s.Version() < version2_5_1 {
		return
	}

	anonReplicaCount, err := getAnonReplicasCount(box.Listen)
	require.NoError(err)

	require.EqualValues(expected.AnonReplicaCount, anonReplicaCount)
}

func TestAnonSlaveJoin(t *testing.T) {
	require := require.New(t)

	box, err := newTntBoxWithEval()
	require.NoError(err)
	defer box.Close()

	skipUnsupportedVersion(t, box)

	s, _ := NewAnonSlave(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
		UUID:     tnt16UUID})

	expected := struct {
		UUID             string
		AnonReplicaCount int
	}{tnt16UUID, 0} // Join doesn't add anon replica to anon replica list but Attach and Subscribe do

	err = s.Join()
	require.NoError(err)
	err = s.Close()
	require.NoError(err)

	// check
	require.NotZero(s.ReplicaSet.UUID)

	// return if tarantool version doesn't have box.info.replication_anon
	if s.Version() < version2_5_1 {
		return
	}

	anonReplicaCount, err := getAnonReplicasCount(box.Listen)
	require.NoError(err)

	require.EqualValues(expected.AnonReplicaCount, anonReplicaCount)
}

func TestAnonSlaveDoubleClose(t *testing.T) {
	require := require.New(t)
	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	skipUnsupportedVersion(t, box)

	s, _ := NewAnonSlave(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
	})
	err = s.Join()
	require.NoError(err)

	// check
	err = s.Close()
	require.NoError(err)
	require.NotPanics(assert.PanicTestFunc(func() {
		err = s.Close()
	}))
	require.NoError(err)
}

func TestAnonSlaveSubscribeSync(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	// setup TestBox
	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	skipUnsupportedVersion(t, box)

	s, _ := NewAnonSlave(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
	})
	// get ReplicaSetUUID
	err = s.Join()
	require.NoError(err)
	assert.Len(s.UUID, UUIDStrLength)
	assert.Len(s.ReplicaSet.UUID, UUIDStrLength)
	err = s.Close()
	require.NoError(err)

	// new instance for the purity of the test
	ns, _ := NewAnonSlave(box.Listen, Options{
		User:           tnt16User,
		Password:       tnt16Pass,
		UUID:           s.UUID,
		ReplicaSetUUID: s.ReplicaSet.UUID,
	})
	defer ns.Close()

	it, err := ns.Subscribe(0)
	require.NoError(err)

	resultChan := make(chan bool, 1)
	go func(it PacketIterator, rch chan bool) {
		var p *Packet
		var err error
		for err != io.EOF {
			p, err = it.Next()
			if err == nil && p != nil {
				rch <- true
			}
			// if we are here something is going wrong
			break
		}
		rch <- false
	}(it, resultChan)

	// check
	timeout := time.After(10 * time.Second)
	select {
	case success := <-resultChan:
		assert.True(success, "there is no packet with insert UUID in cluster space")
	case <-timeout:
		t.Fatal("timeout")
	}
}

func TestAnonSlaveHasNextOnSubscribe(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	// setup TestBox
	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	skipUnsupportedVersion(t, box)

	s, _ := NewAnonSlave(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
	})
	// register in replica set
	err = s.Join()
	require.NoError(err)
	err = s.Close()
	require.NoError(err)

	// new instance for the purity of the test
	ns, _ := NewAnonSlave(box.Listen, Options{
		User:           tnt16User,
		Password:       tnt16Pass,
		UUID:           s.UUID,
		ReplicaSetUUID: s.ReplicaSet.UUID,
	})
	defer ns.Close()

	_, err = ns.Subscribe(0)
	require.NoError(err)

	resultChan := make(chan bool, 1)
	go func(ns *AnonSlave, rch chan bool) {
		for ns.HasNext() {
			rch <- true
		}
		rch <- false
	}(ns, resultChan)

	// check
	timeout := time.After(10 * time.Second)
	select {
	case success := <-resultChan:
		assert.True(success, "there is no packet with insert UUID in cluster space")
	case <-timeout:
		t.Fatal("timeout")
	}
}

func TestAnonSlaveVClock(t *testing.T) {
	require := require.New(t)

	// setup
	box, err := newTntBoxWithEval()
	require.NoError(err)
	defer box.Close()

	skipUnsupportedVersion(t, box)

	tnt, err := Connect(box.Listen, &Options{})
	require.NoError(err)
	defer tnt.Close()

	makesnapshot := &Eval{Expression: luaMakeSnapshot}
	res, err := tnt.Execute(makesnapshot)
	require.NoError(err)
	require.Len(res, 0, "response to make snapshot request contains error")

	// check 1
	s, err := NewAnonSlave(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
		UUID:     tnt16UUID,
	})
	require.NoError(err)
	defer s.Close()

	_, err = s.JoinWithSnap()
	require.NoError(err)
	var (
		count          int
		finalDataCount int
	)

	for ; s.HasNext(); count++ {
		// add new items to tnt concurrently while snap is downloading
		field := fmt.Sprintf("Inserted tuple #%v", count+finalDataCount+2)
		_, err = tnt.Execute(&Insert{
			Space: "tester",
			Tuple: []interface{}{uint(count + 2), field},
		})
		require.NoError(err)
	}

	require.NoError(s.Err())

	joinLSN := s.VClock.LSN()
	t.Logf("Join: %#v", s.VClock)

	it, err := s.Subscribe(s.VClock[1:]...)
	require.NoError(err)
	subscribeLSN := s.VClock.LSN()
	t.Logf("Subscribe: %#v", s.VClock)

	assert.EqualValues(t, count, subscribeLSN-joinLSN)

	// check 2
	for ; count > 0; count-- {
		_, err = it.Next()
		require.NoError(err)
	}

	hasNext := make(chan struct{})
	go func(ch chan struct{}) {
		_, _ = it.Next()
		ch <- struct{}{}
	}(hasNext)

	// check if there is unexpected additional packets
	timer := time.NewTimer(2 * time.Second)
	select {
	case <-hasNext:
		t.Error("Received more packets than expected")
	case <-timer.C:
	}

	assert.EqualValues(t, uint(subscribeLSN), uint(s.VClock.LSN()))
}

func TestAnonSlaveAttach(t *testing.T) {
	require := require.New(t)

	// setup TestBox
	box, err := newTntBoxWithEval()
	require.NoError(err)
	defer box.Close()

	skipUnsupportedVersion(t, box)

	// setup
	s, _ := NewAnonSlave(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
		UUID:     tnt16UUID})

	defer s.Close()

	// check
	it, err := s.Attach()
	require.NoError(err)
	assert.NotNil(t, it)

	expectedAnonReplicaCount := 1

	// return if tarantool version doesn't have box.info.replication_anon
	if s.Version() < version2_5_1 {
		return
	}

	anonReplicaCount, err := getAnonReplicasCount(box.Listen)
	require.NoError(err)

	require.EqualValues(expectedAnonReplicaCount, anonReplicaCount)
}

func TestAnonSlaveAttachAsync(t *testing.T) {
	require := require.New(t)

	// setup TestBox
	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	skipUnsupportedVersion(t, box)

	// setup Slave
	s, _ := NewAnonSlave(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
	})
	respc := make(chan *Packet, 1)
	_, err = s.Attach(respc)
	require.NoError(err)
	defer s.Close()

	out := make(chan *Insert, 32)
	go func(in <-chan *Packet, out chan *Insert) {
		for packet := range in {
			switch q := packet.Request.(type) {
			case *Insert:
				out <- q
			}
		}
	}(respc, out)

	// add new data to TestBox
	tnt, err := Connect(box.Listen, &Options{})
	require.NoError(err)
	defer tnt.Close()
	expected := []interface{}{int64(2), "Client inserted #2"}
	res, err := tnt.Execute(&Insert{
		Space: "tester",
		Tuple: expected,
	})
	require.NoError(err)
	require.Len(res, 1)
	require.Equal(expected, res[0])

	// check
	timeout := time.After(10 * time.Second)
	for {
		select {
		case qi := <-out:
			if qi.Space.(uint) > SpaceSystemMax {
				if num, err := numberToUint64(qi.Tuple[0]); err == nil && num > 1 {
					require.EqualValues(expected, qi.Tuple)
					return
				}
			}
		case <-timeout:
			t.Fatal("Timeout: there is no necessary Insert")
		}
	}
}

func TestAnonSlaveParseOptionsRSParams(t *testing.T) {
	require := require.New(t)

	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	skipUnsupportedVersion(t, box)

	uri := fmt.Sprintf("%v:%v@%v", tnt16User, tnt16Pass, box.Listen)
	tt := []struct {
		opts       Options
		isParseErr bool
		inReplica  bool
	}{
		// good cases
		{Options{UUID: "uuid1"}, false, false},
		{Options{UUID: "uuid1", ReplicaSetUUID: "uuid2"}, false, true},
	}
	for tc, item := range tt {
		s, err := NewAnonSlave(uri, item.opts)
		if item.isParseErr {
			require.Error(err, fmt.Sprintf("case %v", tc+1))
			// prepare handmade for second check
			s = &AnonSlave{Slave{UUID: item.opts.UUID}}
			s.ReplicaSet.UUID = item.opts.ReplicaSetUUID
		} else {
			require.NoError(err, "case %v", tc+1)
		}
		require.Equal(item.inReplica, s.IsInReplicaSet(), "case %v", tc+1)
	}
}

func getAnonReplicasCount(listen string) (count int, err error) {
	tnt, err := Connect(listen, &Options{})
	if err != nil {
		return 0, err
	}
	defer tnt.Close()

	eval := &Eval{Expression: getAnonReplicasExpression}
	response, err := tnt.Execute(eval)
	if err != nil {
		return 0, err
	}

	if len(response) == 0 {
		return 0, nil
	}

	innerData := response[0]
	if len(innerData) == 0 {
		return 0, nil
	}

	var (
		m              map[string]interface{}
		count64        int64
		countInterface interface{}
		ok             bool
	)
	if m, ok = innerData[0].(map[string]interface{}); !ok {
		return 0, fmt.Errorf("unexpected dataType: %T", innerData[0])
	}

	if countInterface, ok = m["count"]; !ok {
		return 0, fmt.Errorf("count is not found inside %v", m)
	}

	if count64, ok = countInterface.(int64); !ok {
		return 0, fmt.Errorf("unexpected dataType of count: %T", countInterface)
	}

	return int(count64), nil
}

func skipUnsupportedVersion(t *testing.T, box *Box) {
	ver, err := box.Version()
	require.NoError(t, err)

	version := strings.Split(ver, ".")
	major, _ := strconv.Atoi(version[0])
	minor, _ := strconv.Atoi(version[1])
	patch, _ := strconv.Atoi(version[2])

	tarantoolVersion := VersionID(uint32(major), uint32(minor), uint32(patch))
	if tarantoolVersion < version2_3_1 {
		t.Skip("old tarantool version. Min Tarantool version for this test is 2.3.1")
	}
}

func newTntBoxWithEval() (*Box, error) {
	config := schemeNewReplicator(tnt16User, tnt16Pass)
	config += schemeSpaceTester()

	guest := "guest"
	config += schemeGrantUserEval(guest)

	return NewBox(config, &BoxOptions{})
}
