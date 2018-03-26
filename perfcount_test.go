package tarantool

import (
	"expvar"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPerfCount(t *testing.T) {
	perf := PerfCount{
		expvar.NewInt("net_read"),
		expvar.NewInt("net_write"),
		expvar.NewInt("net_packets_in"),
		expvar.NewInt("net_packets_out"),
	}

	assert := assert.New(t)
	require := require.New(t)
	config := `
	s = box.schema.space.create('tester', {id = 42})
	box.schema.user.grant('guest', 'write', 'space', 'tester')
	s:create_index('tester_id', {
		type = 'hash',
		parts = {1, 'NUM'}
	})
	`
	box, err := NewBox(config, nil)
	require.NoError(err)
	defer box.Close()

	conn, err := Connect(box.Addr(), &Options{
		DefaultSpace: "tester",
		Perf:         perf,
	})
	require.NoError(err)
	defer conn.Close()

	_, err = conn.Execute(&Replace{
		Tuple: []interface{}{int64(1)},
	})
	require.NoError(err)

	nr := perf.NetRead.Value()
	nw := perf.NetWrite.Value()
	pin := perf.NetPacketsIn.Value()
	pout := perf.NetPacketsOut.Value()

	assert.True(nr > 0)
	assert.True(nw > 0)
	assert.True(pin > 0)
	assert.True(pout > 0)

	tuples, err := conn.Execute(&Select{
		KeyTuple: []interface{}{int64(1)},
	})
	require.NoError(err)
	assert.Equal([][]interface{}{{int64(1)}}, tuples)

	assert.True(perf.NetRead.Value() > nr)
	assert.True(perf.NetWrite.Value() > nw)
	assert.Equal(pin + 1, perf.NetPacketsIn.Value())
	assert.Equal(pout + 1, perf.NetPacketsOut.Value())
}
