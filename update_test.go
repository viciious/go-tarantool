package tarantool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdate(t *testing.T) {
	assert := assert.New(t)

	tarantoolConfig := `
    s = box.schema.space.create('tester')
    s:create_index('primary', {
        type = 'hash',
        parts = {1, 'NUM'}
    })
    t = s:insert({1, 'First record', 15})

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

	data, err := conn.Execute(&Update{
		Space: "tester",
		Index: "primary",
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
	})

	if assert.NoError(err) {
		assert.Equal([][]interface{}{
			{int64(1), "Hello World", int64(32)},
		}, data)
	}

}

func BenchmarkUpdatePack(b *testing.B) {
	d := newPackData(42)
	buf := make([]byte, 0)

	for i := 0; i < b.N; i++ {
		buf, _, _ = (&Update{
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
		}).PackMsg(d, buf)
	}
}
