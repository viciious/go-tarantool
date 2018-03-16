package tarantool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpsert(t *testing.T) {
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

	do := func(connectOptions *Options, query *Select, expected [][]interface{}) {
		conn, err := box.Connect(connectOptions)
		assert.NoError(err)
		assert.NotNil(conn)

		defer conn.Close()

		data, err := conn.Execute(query)

		if assert.NoError(err) {
			assert.Equal(expected, data)
		}
	}

	// test update
	data, err := conn.Execute(&Upsert{
		Space: "tester",
		Tuple: []interface{}{1},
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
		assert.Equal([][]interface{}{}, data)
	}

	// check update
	do(nil,
		&Select{
			Space: "tester",
			Key:   1,
		},
		[][]interface{}{
			{int64(1), "Hello World", int64(32)},
		},
	)

	// test insert
	data, err = conn.Execute(&Upsert{
		Space: "tester",
		Tuple: []interface{}{2, "Second", 16},
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
		assert.Equal([][]interface{}{}, data)
	}

	// check insert
	do(nil,
		&Select{
			Space: "tester",
			Key:   2,
		},
		[][]interface{}{
			{int64(2), "Second", int64(16)},
		},
	)

}

func BenchmarkUpsertPack(b *testing.B) {
	d := newPackData(42)
	buf := make([]byte, 0)

	for i := 0; i < b.N; i++ {
		buf, _ = (&Upsert{
			Space: 1,
			Tuple: []interface{}{1},
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
