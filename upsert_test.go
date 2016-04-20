package tarantool

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpsert(t *testing.T) {
	if os.Getenv("TARANTOOL16") == "" {
		t.Skip("skipping tarantool16 tests")
	}
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

	do := func(connectOptions *Options, query *Select, expected []interface{}) {
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
		assert.Equal([]interface{}{}, data)
	}

	// check update
	do(nil,
		&Select{
			Space: "tester",
			Key:   1,
		},
		[]interface{}{
			[]interface{}{
				uint64(1),
				"Hello World",
				uint64(32),
			},
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
		assert.Equal([]interface{}{}, data)
	}

	// check insert
	do(nil,
		&Select{
			Space: "tester",
			Key:   2,
		},
		[]interface{}{
			[]interface{}{
				uint64(2),
				"Second",
				uint64(16),
			},
		},
	)

}

func BenchmarkUpsertPack(b *testing.B) {
	d, _ := newPackData(42)

	for i := 0; i < b.N; i += 1 {
		(&Upsert{
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
		}).Pack(0, d)
	}
}
