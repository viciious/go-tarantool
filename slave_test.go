package tarantool

import (
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const luaMakeSnapshot = "require('box').snapshot()"

var (
	tnt16User = "username"
	tnt16Pass = "password"
	tnt16UUID = "7c025e42-2394-11e7-aacf-0242ac110002"
)

func schemeNewReplicator(user, pass string) string {
	tmpl := `
	box.once('{user}:role_replication', function()
		box.schema.user.create('{user}', {password = '{pass}', if_not_exists = true})
		box.schema.user.grant('{user}','execute','role','replication', {if_not_exists = true})
		end)
	`
	tmpl = strings.Replace(tmpl, "{user}", user, -1)
	tmpl = strings.Replace(tmpl, "{pass}", pass, -1)
	return tmpl
}

func schemeGrantRoleFunc(role, fn string) string {
	scheme := `
	box.once('{role}:exec_{fn}', function()
		box.schema.role.grant('{role}', 'execute', 'function', '{fn}', {if_not_exists = true})
	end)
	`
	scheme = strings.Replace(scheme, "{role}", role, -1)
	scheme = strings.Replace(scheme, "{fn}", fn, -1)
	return scheme
}

func schemeSpaceTester() string {
	return `
	box.once('space:tester', function()
		s = box.schema.space.create('tester')
		i = s:create_index('primary', {})
		s:insert{1, 'Initial tuple #1'}
		end)
	box.once('guest:write_tester', function()
		box.schema.user.grant('guest','write', 'space', 'tester', {if_not_exists = true})
		end)
	`
}

func newTntBox() (*Box, error) {
	config := schemeNewReplicator(tnt16User, tnt16Pass)
	config += schemeSpaceTester()

	return NewBox(config, &BoxOptions{})
}

func TestTntBoxStart(t *testing.T) {
	if testing.Short() {
		t.Skip("Start this test if Slave Connect will be failed")
	}
	// setup TestBox
	box, err := newTntBox()
	require.NoError(t, err)
	defer box.Close()
	require.NotEmpty(t, box.Listen)
}

func TestTntBoxGuestWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("Start this test if Slave Complex will be failed")
	}
	require := require.New(t)

	// setup TestBox
	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()
	require.NotEmpty(box.Listen)

	// connect as quest
	tnt, err := Connect(box.Listen, &Options{})
	require.NoError(err)
	defer tnt.Close()
	tuple := []interface{}{int64(2), "Client inserted #2"}
	res, err := tnt.Execute(&Insert{
		Space: "tester",
		Tuple: tuple,
	})
	require.NoError(err)

	// check result
	require.Len(res, 1)
	require.Equal(tuple, res[0])
}

func TestSlaveConnect(t *testing.T) {
	require := require.New(t)
	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	// setup
	opts := Options{User: tnt16User, Password: tnt16Pass}
	s, err := NewSlave(box.Listen, opts)
	require.NoError(err)

	// check
	err = s.connect(box.Listen, &opts)
	require.NoError(err)
	s.c.stop()
}

func TestSlaveJoinWithSnapSync(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	expected := struct {
		UUID          string
		ReplicaSetLen int
	}{tnt16UUID, 1 + 1} // one element and one reserved zero index element
	// setup
	s, _ := NewSlave(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
		UUID:     expected.UUID})
	defer s.Close()

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
	assert.Len(s.ReplicaSet.Instances, expected.ReplicaSetLen)
}

func TestSlaveHasNextOnJoin(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	expected := struct {
		UUID          string
		ReplicaSetLen int
	}{tnt16UUID, 1 + 1} // one element and one reserved zero index element
	// setup
	s, _ := NewSlave(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
		UUID:     expected.UUID})
	defer s.Close()

	_, err = s.JoinWithSnap()
	require.NoError(err)

	resultChan := make(chan bool, 1)
	go func(s *Slave, rch chan bool) {
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
	assert.Len(s.ReplicaSet.Instances, expected.ReplicaSetLen)
}

func TestSlaveIsEmptyChan(t *testing.T) {
	s := &Slave{}

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

func TestSlaveJoinWithSnapAsync(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	expected := struct {
		UUID          string
		ReplicaSetLen int
	}{tnt16UUID, 1 + 1} // one element and one reserved zero index element

	// setup
	s, _ := NewSlave(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
		UUID:     expected.UUID})
	defer s.Close()

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
	assert.Len(s.ReplicaSet.Instances, expected.ReplicaSetLen)
}

func TestSlaveJoin(t *testing.T) {
	require := require.New(t)

	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	expected := struct {
		UUID          string
		ReplicaSetLen int
	}{tnt16UUID, 1 + 1} // one element and one reserved zero index element

	s, _ := NewSlave(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
		UUID:     expected.UUID})
	err = s.Join()
	require.NoError(err)
	err = s.Close()
	require.NoError(err)

	// check
	require.NotZero(s.ReplicaSet.UUID)
	require.Len(s.ReplicaSet.Instances, expected.ReplicaSetLen)
}

func TestSlaveDoubleClose(t *testing.T) {
	require := require.New(t)
	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	s, _ := NewSlave(box.Listen, Options{
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

func TestSlaveSubscribeSync(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	// setup TestBox
	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	s, _ := NewSlave(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
	})
	// register in replica set
	err = s.Join()
	require.NoError(err)
	assert.Len(s.UUID, UUIDStrLength)
	assert.Len(s.ReplicaSet.UUID, UUIDStrLength)
	err = s.Close()
	require.NoError(err)

	// new instance for the purity of the test
	ns, _ := NewSlave(box.Listen, Options{
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
				if isUUIDinReplicaSet(p, s.UUID) {
					rch <- true
					return
				}
				continue
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

func TestSlaveHasNextOnSubscribe(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	// setup TestBox
	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	s, _ := NewSlave(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
	})
	// register in replica set
	err = s.Join()
	require.NoError(err)
	err = s.Close()
	require.NoError(err)

	// new instance for the purity of the test
	ns, _ := NewSlave(box.Listen, Options{
		User:           tnt16User,
		Password:       tnt16Pass,
		UUID:           s.UUID,
		ReplicaSetUUID: s.ReplicaSet.UUID,
	})
	defer ns.Close()

	_, err = ns.Subscribe(0)
	require.NoError(err)

	resultChan := make(chan bool, 1)
	go func(ns *Slave, rch chan bool) {
		for ns.HasNext() {
			if isUUIDinReplicaSet(ns.Packet(), ns.UUID) {
				rch <- true
				return
			}
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

func isUUIDinReplicaSet(p *Packet, uuid string) bool {
	if p == nil || len(uuid) == 0 {
		return false
	}
	switch p.Cmd {
	case InsertCommand:
		q := p.Request.(*Insert)
		switch q.Space {
		case SpaceCluster:
			if uuid == q.Tuple[1].(string) {
				return true
			}
		}
	}
	return false
}

func TestSlaveVClock(t *testing.T) {
	require := require.New(t)

	// setup
	guest := "guest"
	config := schemeNewReplicator(tnt16User, tnt16Pass)
	config += schemeSpaceTester()
	// for making snapshot
	config += schemeGrantUserEval(guest)

	box, err := NewBox(config, &BoxOptions{})
	require.NoError(err)
	defer box.Close()

	tnt, err := Connect(box.Listen, &Options{})
	require.NoError(err)
	defer tnt.Close()

	makesnapshot := &Eval{Expression: luaMakeSnapshot}
	res, err := tnt.Execute(makesnapshot)
	require.NoError(err)
	require.Len(res, 0, "response to make snapshot request contains error")

	// check 1
	s, err := NewSlave(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
		UUID:     tnt16UUID,
	})
	require.NoError(err)
	defer s.Close()

	_, err = s.JoinWithSnap()
	require.NoError(err)
	count := 0
	for s.HasNext() {
		// add new items to tnt concurrently while snap is downloading
		field := fmt.Sprintf("Inserted tuple #%v", count+2)
		_, err = tnt.Execute(&Insert{
			Space: "tester",
			Tuple: []interface{}{count + 2, field},
		})
		require.NoError(err)
		count++
	}
	require.NoError(s.Err())
	joinLSN := s.VClock.LSN()
	t.Logf("Join: %#v", s.VClock)
	it, err := s.Subscribe(s.VClock[1:]...)
	require.NoError(err)
	subscribeLSN := s.VClock.LSN()
	t.Logf("Subscribe: %#v", s.VClock)
	assert.EqualValues(t, count, subscribeLSN-joinLSN-1)

	// check 2
	for ; count >= 0; count-- {
		_, err = it.Next()
		require.NoError(err)
	}
	assert.EqualValues(t, subscribeLSN, s.VClock.LSN())
}

func TestSlaveAttach(t *testing.T) {
	require := require.New(t)

	// setup TestBox
	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	// setup
	s, _ := NewSlave(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
		UUID:     tnt16UUID})

	// check
	it, err := s.Attach()
	require.NoError(err)
	assert.NotNil(t, it)

	// shutdown
	err = s.Close()
	require.NoError(err)
}

func TestSlaveAttachAsync(t *testing.T) {
	require := require.New(t)

	// setup TestBox
	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	// setup Slave
	s, _ := NewSlave(box.Listen, Options{
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

func TestSlaveParseOptionsRSParams(t *testing.T) {
	require := require.New(t)

	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

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
		s, err := NewSlave(uri, item.opts)
		if item.isParseErr {
			require.Error(err, fmt.Sprintf("case %v", tc+1))
			// prepare handmade for second check
			s = &Slave{UUID: item.opts.UUID}
			s.ReplicaSet.UUID = item.opts.ReplicaSetUUID
		} else {
			require.NoError(err, "case %v", tc+1)
		}
		require.Equal(item.inReplica, s.IsInReplicaSet(), "case %v", tc+1)
	}
}

func TestSlaveLastSnapVClock(t *testing.T) {
	require := require.New(t)

	// setup
	user, role, luaDir := "guest", "replication", "lua"
	luaInit, err := ioutil.ReadFile(filepath.Join("testdata", "init.lua"))
	require.NoError(err)
	config := string(luaInit)
	config += schemeNewReplicator(tnt16User, tnt16Pass)
	config += schemeGrantRoleFunc(role, procLUALastSnapVClock)
	// for making snapshot
	config += schemeGrantUserEval(user)

	box, err := NewBox(config, &BoxOptions{WorkDir: luaDir})
	require.NoError(err)
	defer box.Close()

	// add replica to replica set
	s, err := NewSlave(box.Listen, Options{User: tnt16User, Password: tnt16Pass})
	require.NoError(err)
	defer s.Close()
	err = s.Join()
	require.NoError(err)

	// check init snapshot
	expected := NewVectorClock(0)
	vc, err := s.LastSnapVClock()
	require.NoError(err)
	defer s.Close()
	assert.Equal(t, expected, vc, "init snapshot")

	// make snapshot
	tnt, err := Connect(box.Listen, &Options{})
	require.NoError(err)
	defer tnt.Close()
	makesnapshot := &Eval{Expression: luaMakeSnapshot}
	res, err := tnt.Execute(makesnapshot)
	require.NoError(err)
	require.Len(res, 0, "response to make snapshot request contains error")

	// check newly generated snapshot
	vc, err = s.LastSnapVClock()
	require.NoError(err)

	require.NotEqual(expected, vc)
	require.True(vc.Has(2))
	require.Zero(vc[2], "replica clock should be zero")
}

func TestVectorClock(t *testing.T) {
	assert := assert.New(t)
	expected := make([]uint64, VClockMax)
	expectedLSN := uint64(0)
	for i := range expected {
		lsn := uint64(i * 10)
		expected[i] = lsn
		expectedLSN += lsn
	}

	vc := NewVectorClock()
	// zebra filling is special test
	for i := 0; i < VClockMax; i = i + 2 {
		assert.True(vc.Follow(uint32(i), uint64(10*i)), "id=%v", i)
	}
	for i := 1; i < VClockMax; i = i + 2 {
		assert.True(vc.Follow(uint32(i), uint64(10*i)), "id=%v", i)
	}
	// updating existing lsns
	for i := VClockMax - 1; i >= 0; i-- {
		assert.True(vc.Follow(uint32(i), uint64(10*i)), "id=%v", i)
	}
	assert.EqualValues(expected, vc)

	assert.False(vc.Follow(VClockMax, 0), "VClockMax")
	assert.False(vc.Follow(VClockMax+1, 0), "VClockMax+1")

	assert.EqualValues(expected, vc)

	assert.Equal(expectedLSN, vc.LSN(), "LSN")
}

func TestReplicaSet(t *testing.T) {
	assert := assert.New(t)
	expected := make([]string, ReplicaSetMaxSize)
	uuidgen := func(i int) string { return fmt.Sprintf("%v%012v", tnt16UUID[:UUIDStrLength-12], i) }
	for i := range expected {
		expected[i] = uuidgen(i * 10)
	}

	rs := NewReplicaSet()
	// zebra filling is special test
	for i := 0; i < ReplicaSetMaxSize; i = i + 2 {
		assert.True(rs.SetInstance(uint32(i), uuidgen(10*i)), "id=%v", i)
	}
	for i := 1; i < ReplicaSetMaxSize; i = i + 2 {
		assert.True(rs.SetInstance(uint32(i), uuidgen(10*i)), "id=%v", i)
	}
	// updating existing lsns
	for i := ReplicaSetMaxSize - 1; i >= 0; i-- {
		assert.True(rs.SetInstance(uint32(i), uuidgen(10*i)), "id=%v", i)
	}
	assert.EqualValues(expected, rs.Instances)

	assert.False(rs.SetInstance(ReplicaSetMaxSize, tnt16UUID), "ReplicaSetMax")
	assert.False(rs.SetInstance(ReplicaSetMaxSize+1, tnt16UUID), "ReplicaSetMax+1")
	assert.False(rs.SetInstance(0, ""), "Empty UUID")

	assert.EqualValues(expected, rs.Instances)
}
