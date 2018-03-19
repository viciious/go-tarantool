package tarantool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCall(t *testing.T) {
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
    
    function sel_all()
        return box.space.tester:select{}
    end

    function sel_name(tester_id, name)
        return box.space.tester.index.id_name:select{tester_id, name}
    end

    box.schema.func.create('sel_all', {if_not_exists = true})
    box.schema.func.create('sel_name', {if_not_exists = true})

    box.schema.user.grant('guest', 'execute', 'function', 'sel_all', {if_not_exists = true})
    box.schema.user.grant('guest', 'execute', 'function', 'sel_name', {if_not_exists = true})    
    `

	box, err := NewBox(tarantoolConfig, nil)
	if !assert.NoError(err) {
		return
	}
	defer box.Close()

	do := func(connectOptions *Options, query *Call, expected [][]interface{}) {
		var buf []byte

		conn, err := box.Connect(connectOptions)
		assert.NoError(err)
		assert.NotNil(conn)

		defer conn.Close()

		buf, err = query.PackMsg(conn.packData, buf)

		if assert.NoError(err) {
			var query2 = &Call{}
			err = query2.UnmarshalBinary(buf)

			if assert.NoError(err) {
				assert.Equal(query.Name, query2.Name)
				assert.Equal(query.Tuple, query2.Tuple)
			}
		}

		data, err := conn.Execute(query)

		if assert.NoError(err) {
			assert.Equal(expected, data)
		}
	}

	// call sel_all without params
	do(nil,
		&Call{
			Name: "sel_all",
		},
		[][]interface{}{
			{int64(1), "First record"},
			{int64(2), "Music"},
			{int64(3), "Length", int64(93)},
		},
	)

	// call sel_name with params
	do(nil,
		&Call{
			Name:  "sel_name",
			Tuple: []interface{}{int64(2), "Music"},
		},
		[][]interface{}{
			{int64(2), "Music"},
		},
	)

}

func BenchmarkCallPack(b *testing.B) {
	buf := make([]byte, 0)
	d := newPackData(42)
	for i := 0; i < b.N; i++ {
		buf, _ = (&Call{Name: "sel_all"}).PackMsg(d, buf)
	}
}
