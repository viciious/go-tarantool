package tarantool

import (
	"testing"

	"strings"

	"fmt"

	"time"

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
	s = box.schema.space.create('tester')
	i = s:create_index('primary', {})
	s:insert{1, 'Initial tuple #1'}
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
		t.Skip("Start this test if Shadow Connect will be failed")
	}
	// setup TestBox
	box, err := newTntBox()
	require.NoError(t, err)
	defer box.Close()
	require.NotEmpty(t, box.Listen)
}

func TestTntBoxGuestWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("Start this test if Shadow Complex will be failed")
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

func TestShadowConnect(t *testing.T) {
	require := require.New(t)
	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	// setup
	opts := Options{User: tnt16User, Password: tnt16Pass}
	s, err := NewShadow(box.Listen, opts)
	require.NoError(err)

	// check
	err = s.connect(box.Listen, &opts)
	require.NoError(err)
	s.c.stop()
}

func TestShadowJoin(t *testing.T) {
	require := require.New(t)

	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	expected := struct {
		UUID          string
		ReplicaSetLen int
	}{tnt16UUID, 1}

	s, _ := NewShadow(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
		UUID:     expected.UUID})
	err = s.Join()
	require.NoError(err)
	err = s.Detach()
	require.NoError(err)

	// check
	require.NotZero(s.ReplicaSet.UUID)
	require.Len(s.ReplicaSet.Instances, expected.ReplicaSetLen)
}

func TestShadowDoubleDetach(t *testing.T) {
	require := require.New(t)
	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	s, _ := NewShadow(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
	})
	err = s.Join()
	require.NoError(err)

	// check
	err = s.Detach()
	require.NoError(err)
	require.NotPanics(assert.PanicTestFunc(func() {
		err = s.Detach()
	}))
	require.NoError(err)
}

func TestShadowSubscribe(t *testing.T) {
	require := require.New(t)

	// setup TestBox
	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	s, _ := NewShadow(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
	})
	// register in replica set
	err = s.Join()
	require.NoError(err)
	err = s.Detach()
	require.NoError(err)

	// new instance for the purity of the test
	ns, _ := NewShadow(box.Listen, Options{
		User:           tnt16User,
		Password:       tnt16Pass,
		UUID:           s.UUID,
		ReplicaSetUUID: s.ReplicaSet.UUID,
	})
	respc, err := ns.Subscribe(0)
	require.NoError(err)

	// check
	timeout := time.After(10 * time.Second)
	select {
	case <-respc:
		break
	case <-timeout:
		t.Fatal("Timeout: there is no necessary xlog.")
	}

	// shutdown
	err = ns.Detach()
	require.NoError(err)
}

func TestShadowAttach(t *testing.T) {
	require := require.New(t)

	// setup TestBox
	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	// setup
	s, _ := NewShadow(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
		UUID:     tnt16UUID})

	// check
	_, err = s.Attach(0)
	require.NoError(err)

	// shutdown
	err = s.Detach()
	require.NoError(err)
}

func TestShadowComplex(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	require := require.New(t)

	// setup TestBox
	box, err := newTntBox()
	require.NoError(err)
	defer box.Close()

	// setup Shadow
	s, _ := NewShadow(box.Listen, Options{
		User:     tnt16User,
		Password: tnt16Pass,
	})
	respc, err := s.Attach(2)
	require.NoError(err)
	defer s.Detach()

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

func TestShadowParseOptionsRSParams(t *testing.T) {
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
		s, err := NewShadow(uri, item.opts)
		if item.isParseErr {
			require.Error(err, "case %v", tc+1)
			// prepare handmade for second check
			s = &Shadow{UUID: item.opts.UUID}
			s.ReplicaSet.UUID = item.opts.ReplicaSetUUID
		} else {
			require.NoError(err, "case %v", tc+1)
		}
		require.Equal(item.inReplica, s.IsInReplicaSet(), "case %v", tc+1)
	}
}
