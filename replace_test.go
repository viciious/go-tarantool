package tarantool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReplace(t *testing.T) {
	assert := assert.New(t)

	tarantoolConfig := `
    local s = box.schema.space.create('tester', {id = 42})
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
		var buf []byte

		buf, err = query.packMsg(conn.packData, buf)

		if assert.NoError(err) {
			var query2 = &Replace{}
			_, err = query2.UnmarshalMsg(buf)

			if assert.NoError(err) {
				assert.Equal(uint(42), query2.Space)
				assert.Equal(query.Tuple, query2.Tuple)
			}
		}

		return conn.Execute(query)
	}

	data, err := do(&Replace{
		Space: "tester",
		Tuple: []interface{}{int64(4), "Hello"},
	})

	if assert.NoError(err) {
		assert.Equal([][]interface{}{
			{
				int64(4),
				"Hello",
			},
		}, data)
	}

	data, err = do(&Replace{
		Space: "tester",
		Tuple: []interface{}{int64(4), "World"},
	})

	if assert.NoError(err) {
		assert.Equal([][]interface{}{
			{
				int64(4),
				"World",
			},
		}, data)
	}
}

func BenchmarkReplacePack(b *testing.B) {
	buf := make([]byte, 0)
	for i := 0; i < b.N; i++ {
		buf, _ = (&Replace{Tuple: []interface{}{3, "Hello world"}}).MarshalMsg(buf[:0])
	}
}
