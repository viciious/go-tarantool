package tarantool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnect(t *testing.T) {
	assert := assert.New(t)

	box, err := NewBox("", nil)
	assert.NoError(err)
	defer box.Close()

	conn, err := Connect(box.Addr(), nil)
	if !assert.NoError(err) {
		return
	}
	defer conn.Close()

	assert.Contains(string(conn.Greeting.Version), "Tarantool")
}

func TestDefaultSpace(t *testing.T) {
	assert := assert.New(t)
	config := `
	s = box.schema.space.create('tester', {id = 42})
	s:create_index('tester_id', {
		type = 'hash',
		parts = {1, 'NUM'}
	})
	t = s:insert({1})
	`
	box, err := NewBox(config, nil)
	assert.NoError(err)
	defer box.Close()

	conn, err := Connect(box.Addr(), &Options{
		DefaultSpace: "tester",
	})

	assert.NoError(err)
	defer conn.Close()

	tuples, err := conn.Execute(&Select{
		Key:   1,
		Index: "tester_id",
	})
	assert.NoError(err)
	assert.Equal([][]interface{}{
		[]interface{}{uint64(1)},
	}, tuples)
}
