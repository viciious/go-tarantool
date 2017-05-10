package tarantool

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnect(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	box, err := NewBox("", nil)
	require.NoError(err)
	defer box.Close()

	conn, err := Connect(box.Addr(), nil)
	require.NoError(err)
	defer conn.Close()

	assert.Contains(string(conn.Greeting.Version), "Tarantool")
}

func TestDefaultSpace(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	config := `
	s = box.schema.space.create('tester', {id = 42})
	s:create_index('tester_id', {
		type = 'hash',
		parts = {1, 'NUM'}
	})
	t = s:insert({1})
	`
	box, err := NewBox(config, nil)
	require.NoError(err)
	defer box.Close()

	conn, err := Connect(box.Addr(), &Options{
		DefaultSpace: "tester",
	})
	require.NoError(err)
	defer conn.Close()

	tuples, err := conn.Execute(&Select{
		Key:   1,
		Index: "tester_id",
	})
	require.NoError(err)
	assert.Equal([][]interface{}{{uint64(1)}}, tuples)
}
