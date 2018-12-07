package tarantool

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReplace(t *testing.T) {
	assert := assert.New(t)

	tarantoolConfig := `
    s = box.schema.space.create('tester', {id = 42})
    s:create_index('primary', {
        type = 'hash',
        parts = {1, 'NUM'}
    })

    box.schema.user.create('writer', {password = 'writer'})
	box.schema.user.grant('writer', 'write', 'space', 'tester')
    `

	box, err := NewBox(tarantoolConfig, nil)
	if !assert.NoError(err) {
		return
	}
	defer box.Close()

	conn, err := box.Connect(&Options{
		User:     "writer",
		Password: "writer",
	})
	assert.NoError(err)
	assert.NotNil(conn)

	defer conn.Close()

	do := func(query *Replace) ([][]interface{}, error) {
		var err error
		var buf bytes.Buffer

		_, err = query.Pack(conn.packData, &buf)

		if assert.NoError(err) {
			var query2 = &Replace{}
			err = query2.Unpack(&buf)

			if assert.NoError(err) {
				assert.Equal(42, query2.Space)
				assert.Equal(query.Tuple, query2.Tuple)
			}
		}

		return conn.Execute(query)
	}

	data, err := do(&Replace{
		Space: "tester",
		Tuple: []interface{}{int8(4), "Hello"},
	})

	if assert.NoError(err) {
		assert.Equal([][]interface{}{
			{
				uint8(4),
				"Hello",
			},
		}, data)
	}

	data, err = do(&Replace{
		Space: "tester",
		Tuple: []interface{}{int8(4), "World"},
	})

	if assert.NoError(err) {
		assert.Equal([][]interface{}{
			{
				uint8(4),
				"World",
			},
		}, data)
	}
}

func BenchmarkReplacePack(b *testing.B) {
	d := newPackData(42)

	for i := 0; i < b.N; i++ {
		pp := packetPool.Get()
		(&Replace{Tuple: []interface{}{3, "Hello world"}}).Pack(d, &pp.buffer)
		pp.Release()
	}
}
