package tarantool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDelete(t *testing.T) {
	assert := assert.New(t)

	tarantoolConfig := `
    local s = box.schema.space.create('tester', {id = 42})
    s:create_index('primary', {
        type = 'hash',
        parts = {1, 'NUM'}
    })

    s = box.schema.space.create('tester2', {id = 43})
    s:create_index('primary', {
        type = 'tree',
        parts = {1, 'NUM', 2, 'STR'},
        unique = true
    })

    box.schema.user.create('writer', {password = 'writer'})
    box.schema.user.grant('writer', 'read,write', 'space', 'tester')
    box.schema.user.grant('writer', 'read,write', 'space', 'tester2')
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

	do := func(query *Delete) ([][]interface{}, error) {
		var err error
		var buf []byte

		buf, err = query.packMsg(conn.packData, buf)

		if assert.NoError(err) {
			var query2 = &Delete{}
			_, err = query2.UnmarshalMsg(buf)

			if assert.NoError(err) {
				switch query.Space.(string) {
				case "tester":
					assert.Equal(uint(42), query2.Space)
				case "tester2":
					assert.Equal(uint(43), query2.Space)
				}

				if query.Key != nil {
					assert.Equal(query.Key, query2.Key)
				}
				if query.KeyTuple != nil {
					assert.Equal(query.KeyTuple, query2.KeyTuple)
				}
			}
		}

		return conn.Execute(query)
	}

	data, err := conn.Execute(&Replace{
		Space: "tester",
		Tuple: []interface{}{int64(4), "Hello"},
	})

	assert.NoError(err)

	data, err = do(&Delete{
		Space: "tester",
		Key:   int64(4),
	})

	if assert.NoError(err) {
		assert.Equal([][]interface{}{
			{
				int64(4),
				"Hello",
			},
		}, data)
	}

	data, err = conn.Execute(&Select{
		Space:    "tester",
		KeyTuple: []interface{}{int64(4)},
	})
	if assert.NoError(err) {
		assert.Equal([][]interface{}{}, data)
	}

	data, err = conn.Execute(&Replace{
		Space: "tester2",
		Tuple: []interface{}{int64(4), "World"},
	})

	assert.NoError(err)

	data, err = do(&Delete{
		Space:    "tester2",
		KeyTuple: []interface{}{int64(4), "World"},
	})

	if assert.NoError(err) {
		assert.Equal([][]interface{}{
			{
				int64(4),
				"World",
			},
		}, data)
	}

	data, err = conn.Execute(&Select{
		Space:    "tester2",
		KeyTuple: []interface{}{int64(4), "World"},
	})
	if assert.NoError(err) {
		assert.Equal([][]interface{}{}, data)
	}

	data, err = do(&Delete{
		Space:    "tester2",
		KeyTuple: []interface{}{int64(4), "World"},
	})

	assert.NoError(err)
}

func BenchmarkDeletePack(b *testing.B) {
	buf := make([]byte, 0)
	for i := 0; i < b.N; i++ {
		buf, _ = (&Delete{KeyTuple: []interface{}{3, "Hello world"}}).MarshalMsg(buf[:0])
	}
}
