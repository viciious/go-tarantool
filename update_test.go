package tarantool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdate(t *testing.T) {
	assert := assert.New(t)

	tarantoolConfig := `
    local s = box.schema.space.create('tester')
    s:create_index('primary', {
        type = 'hash',
        parts = {1, 'NUM'}
    })
    local t = s:insert({1, 'First record', 15})

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

	do := func(conn *Connection, query *Update, expected [][]interface{}) {
		var err error
		var buf []byte

		buf, err = query.packMsg(conn.packData, buf)

		if assert.NoError(err) {
			var query2 = &Update{}
			_, err = query2.UnmarshalMsg(buf)
			if assert.NoError(err) {
				assert.Equal(uint(512), query2.Space)
				if query.Key != nil {
					switch query.Key.(type) {
					case int:
						assert.Equal(query.Key, query2.Key)
					default:
						assert.Equal(query.Key, query2.Key)
					}
				}
				if query.KeyTuple != nil {
					assert.Equal(query.KeyTuple, query2.KeyTuple)
				}
				if query.Index != nil {
					switch query.Index.(type) {
					case string:
						assert.Equal(conn.packData.indexMap[512][query.Index.(string)], uint64(query2.Index.(uint)))
					default:
						assert.Equal(query.Index, query2.Index)
					}
				}
				assert.Equal(query.Set, query2.Set)
			}
		}

		data, err := conn.Execute(query)
		if assert.NoError(err) {
			assert.Equal(expected, data)
		}
	}

	do(conn, &Update{
		Space: "tester",
		Index: "primary",
		Key:   int64(1),
		Set: []Operator{
			&OpAdd{
				Field:    2,
				Argument: 17,
			},
			&OpAssign{
				Field:    1,
				Argument: "Hello World",
			},
		}},
		[][]interface{}{
			{int64(1), "Hello World", int64(32)},
		})
}

func BenchmarkUpdatePack(b *testing.B) {
	buf := make([]byte, 0)

	for i := 0; i < b.N; i++ {
		buf, _ = (&Update{
			Space: 1,
			Index: 0,
			Key:   1,
			Set: []Operator{
				&OpAdd{
					Field:    2,
					Argument: 17,
				},
				&OpAssign{
					Field:    1,
					Argument: "Hello World",
				},
			},
		}).MarshalMsg(buf[:0])
	}
}
