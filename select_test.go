package tnt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelect(t *testing.T) {
	assert := assert.New(t)

	tarantoolConfig := `
    s = box.schema.space.create('tester', {id = 42})
    s:create_index('tester_id', {
        type = 'hash',
        parts = {1, 'NUM'}
    })
	s:create_index('tester_name', {
        type = 'hash',
        parts = {2, 'STR'}
    })
    t = s:insert({1, 'First record'})
    t = s:insert({2, 'Music'})
    t = s:insert({3, 'Length', 93})
    `

	box, err := NewBox(tarantoolConfig, nil)
	if !assert.NoError(err) {
		return
	}
	defer box.Close()

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

	// simple select
	do(nil,
		&Select{
			Space: 42,
			Key:   3,
		},
		[]interface{}{
			[]interface{}{
				uint32(0x3),
				"Length",
				uint32(0x5d),
			},
		},
	)

	// select with space name
	do(nil,
		&Select{
			Space: "tester",
			Key:   3,
		},
		[]interface{}{
			[]interface{}{
				uint32(0x3),
				"Length",
				uint32(0x5d),
			},
		},
	)

	// select with index name
	do(nil,
		&Select{
			Space: "tester",
			Index: "tester_name",
			Key:   "Music",
		},
		[]interface{}{
			[]interface{}{
				uint32(0x2),
				"Music",
			},
		},
	)

}

func BenchmarkSelectPack(b *testing.B) {
	d, _ := newPackData(42)

	for i := 0; i < b.N; i += 1 {
		(&Select{Key: 3}).Pack(0, d)
	}
}
