package tarantool

import (
	"bytes"
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
	s:create_index('id_name', {
        type = 'hash',
        parts = {1, 'NUM', 2, 'STR'},
        unique = true
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

	do := func(connectOptions *Options, query *Select, expected [][]interface{}) {
		var err error
		var buf bytes.Buffer

		conn, err := box.Connect(connectOptions)
		assert.NoError(err)
		assert.NotNil(conn)

		defer conn.Close()

		_, err = query.Pack(conn.packData, &buf)

		if assert.NoError(err) {
			var query2 = &Select{}
			err = query2.Unpack(&buf)

			if assert.NoError(err) {
				assert.Equal(42, query2.Space)
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
						assert.Equal(conn.packData.indexMap[42][query.Index.(string)], uint64(query2.Index.(int)))
					default:
						assert.Equal(query.Index, query2.Index)
					}
				}
				assert.Equal(query.Iterator, query2.Iterator)
			}
		}

		data, err := conn.Execute(query)

		if assert.NoError(err) {
			assert.Equal(expected, data)
		}
	}

	// simple select
	do(nil,
		&Select{
			Space: 42,
			Key:   uint64(3),
		},
		[][]interface{}{
			[]interface{}{
				uint64(0x3),
				"Length",
				uint64(0x5d),
			},
		},
	)

	// select with space name
	do(nil,
		&Select{
			Space: "tester",
			Key:   uint64(3),
		},
		[][]interface{}{
			[]interface{}{
				uint64(0x3),
				"Length",
				uint64(0x5d),
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
		[][]interface{}{
			[]interface{}{
				uint64(0x2),
				"Music",
			},
		},
	)

	// composite key
	do(nil,
		&Select{
			Space:    42,
			Index:    "id_name",
			KeyTuple: []interface{}{uint64(2), "Music"},
		},
		[][]interface{}{
			[]interface{}{
				uint64(0x2),
				"Music",
			},
		},
	)

	// composite key empty response
	do(nil,
		&Select{
			Space:    42,
			Index:    "id_name",
			KeyTuple: []interface{}{uint64(2), "Length"},
		},
		[][]interface{}{},
	)
	// iterate all using NUM index
	do(nil,
		&Select{
			Space:    42,
			Iterator: IterAll,
		},
		[][]interface{}{
			[]interface{}{
				uint64(1),
				"First record",
			},
			[]interface{}{
				uint64(2),
				"Music",
			},
			[]interface{}{
				uint64(3),
				"Length",
				uint64(93),
			},
		},
	)
	// iterate all using STR index
	do(nil,
		&Select{
			Space:    42,
			Index:    "tester_name",
			Iterator: IterAll,
		},
		[][]interface{}{
			[]interface{}{
				uint64(2),
				"Music",
			},
			[]interface{}{
				uint64(3),
				"Length",
				uint64(93),
			},
			[]interface{}{
				uint64(1),
				"First record",
			},
		},
	)
	// iterate Eq using STR index
	do(nil,
		&Select{
			Space:    42,
			Index:    "tester_name",
			Key:      "Length",
			Iterator: IterEq,
		},
		[][]interface{}{
			[]interface{}{
				uint64(3),
				"Length",
				uint64(93),
			},
		},
	)

}

func BenchmarkSelectPack(b *testing.B) {
	d, _ := newPackData(42)

	for i := 0; i < b.N; i += 1 {
		pp := packetPool.Get()
		(&Select{Key: 3}).Pack(d, &pp.buffer)
		pp.Release()
	}
}
