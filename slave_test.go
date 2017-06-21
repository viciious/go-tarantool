package tarantool

import (
	"testing"

	"strings"

	"fmt"

	"time"

	"io"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	tnt16User = "username"
	tnt16Pass = "password"
	tnt16UUID = "7c025e42-2394-11e7-aacf-0242ac110002"
)

func replicatorConfig(user, pass string) string {
	tmpl := `
	box.once('{user}:role_replication', function()
		box.schema.user.create('{user}', {password = '{pass}'})
		box.schema.user.grant('{user}','execute','role','replication', {if_not_exists = true})
		end)
	`
	tmpl = strings.Replace(tmpl, "{user}", user, -1)
	tmpl = strings.Replace(tmpl, "{pass}", pass, -1)
	return tmpl
}

func spaceTester() string {
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
	config := replicatorConfig(tnt16User, tnt16Pass)
	config += spaceTester()

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
	tuple := []interface{}{uint64(2), "Client inserted #2"}
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
	}{tnt16UUID, 1}
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
		t.Fatal("Timeout: there is no necessary xlog.")
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
	}{tnt16UUID, 1}

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
	}{tnt16UUID, 1}

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

func TestSlaveSubscribeAsync(t *testing.T) {
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
	respc := make(chan *Packet, 1)
	it, err := ns.Subscribe(0, respc)
	require.NoError(err)

	// check
	timeout := time.After(10 * time.Second)
loop:
	for {
		select {
		case p := <-respc:
			require.NotNil(p)
			if isUUIDinReplicaSet(p, s.UUID) {
				break loop
			}
		case <-timeout:
			t.Fatal("Timeout: there is no necessary xlog.")
			break loop
		}
	}
	assert.Nil(it)
}

func isUUIDinReplicaSet(p *Packet, uuid string) bool {
	if p == nil || len(uuid) == 0 {
		return false
	}
	switch p.code {
	case InsertRequest:
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
	it, err := s.Attach(0)
	require.NoError(err)
	assert.NotNil(t, it)

	// shutdown
	err = s.Close()
	require.NoError(err)
}

func TestSlaveComplex(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
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
	_, err = s.Attach(2, respc)
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
	expected := []interface{}{uint64(2), "Client inserted #2"}
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
			if qi.Space.(int) > SpaceSystemMax {
				if num, ok := qi.Tuple[0].(uint64); ok && num > 1 {
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
			require.Error(err, "case %v", tc+1)
			// prepare handmade for second check
			s = &Slave{UUID: item.opts.UUID}
			s.ReplicaSet.UUID = item.opts.ReplicaSetUUID
		} else {
			require.NoError(err, "case %v", tc+1)
		}
		require.Equal(item.inReplica, s.IsInReplicaSet(), "case %v", tc+1)
	}
}
