package tarantool

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCall17(t *testing.T) {
	assert := assert.New(t)

	tarantoolConfig := `
    local s = box.schema.space.create('tester', {id = 42})
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
    local t = s:insert({1, 'First record'})
    t = s:insert({2, 'Music'})
    t = s:insert({3, 'Length', 93})
    
    function sel_all()
        return box.space.tester:select{}
    end

    function sel_name(tester_id, name)
        return box.space.tester.index.id_name:select{tester_id, name}
    end

	function call_case_1()
		return 1
	end

	function call_case_2()
		return 1, 2, 3
	end

	function call_case_3()
		return true
	end

	function call_case_4()
		return nil
	end

	function call_case_5()
		return {}
	end

	function call_case_6()
		return {1}
	end

	function call_case_7()
		return {1, 2, 3}
	end

	function call_case_8()
		return {1, 2, 3}, {'a', 'b', 'c'}, {true, false}
	end

	function call_case_9()
		return {key1 = 'value1', key2 = 'value2'}
	end

	local number_of_extra_cases = 9

    box.schema.func.create('sel_all', {if_not_exists = true})
	box.schema.func.create('sel_name', {if_not_exists = true})
	for i = 1, number_of_extra_cases do
		box.schema.func.create('call_case_'..i, {if_not_exists = true})
	end

    box.schema.user.grant('guest', 'execute', 'function', 'sel_all', {if_not_exists = true})
	box.schema.user.grant('guest', 'execute', 'function', 'sel_name', {if_not_exists = true})
	for i = 1, number_of_extra_cases do
		box.schema.user.grant('guest', 'execute', 'function', 'call_case_'..i, {if_not_exists = true})
	end
    `

	box, err := NewBox(tarantoolConfig, nil)
	if !assert.NoError(err) {
		return
	}
	defer box.Close()

	ver, err := box.Version()
	if !assert.NoError(err) {
		return
	}
	if strings.HasPrefix(ver, "1.6") {
		return // requires tarantool >= 1.7.2
	}

	do := func(connectOptions *Options, query *Call17, expected [][]interface{}) {
		var buf []byte

		conn, err := box.Connect(connectOptions)
		assert.NoError(err)
		assert.NotNil(conn)

		defer conn.Close()

		buf, err = query.MarshalMsg(nil)

		if assert.NoError(err) {
			var query2 = &Call17{}
			_, err = query2.UnmarshalMsg(buf)

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
		&Call17{
			Name: "sel_all",
		},
		[][]interface{}{
			[]interface{}{
				[]interface{}{int64(1), "First record"},
				[]interface{}{int64(2), "Music"},
				[]interface{}{int64(3), "Length", int64(93)},
			},
		},
	)

	// call sel_name with params
	do(nil,
		&Call17{
			Name:  "sel_name",
			Tuple: []interface{}{int64(2), "Music"},
		},
		[][]interface{}{
			[]interface{}{
				[]interface{}{int64(2), "Music"},
			},
		},
	)

	// For stored procedures the result is returned in the same way as eval (in certain cases).
	// Note that returning arrays (also an empty table) is a special case.

	// scalar 1
	do(nil,
		&Call17{
			Name: "call_case_1",
		},
		[][]interface{}{
			[]interface{}{int64(1)},
		},
	)

	// multiple scalars
	do(nil,
		&Call17{
			Name: "call_case_2",
		},
		[][]interface{}{
			[]interface{}{int64(1)}, []interface{}{int64(2)}, []interface{}{int64(3)},
		},
	)

	// scalar true
	do(nil,
		&Call17{
			Name: "call_case_3",
		},
		[][]interface{}{
			[]interface{}{true},
		},
	)

	// scalar nil
	do(nil,
		&Call17{
			Name: "call_case_4",
		},
		[][]interface{}{
			[]interface{}{nil},
		},
	)

	// empty table
	do(nil,
		&Call17{
			Name: "call_case_5",
		},
		[][]interface{}{
			[]interface{}{},
		},
	)

	// array with len 1 (similar to case 1)
	do(nil,
		&Call17{
			Name: "call_case_6",
		},
		[][]interface{}{
			[]interface{}{int64(1)},
		},
	)

	// single array with len 3
	do(nil,
		&Call17{
			Name: "call_case_7",
		},
		[][]interface{}{
			[]interface{}{int64(1), int64(2), int64(3)},
		},
	)

	// multiple arrays
	do(nil,
		&Call17{
			Name: "call_case_8",
		},
		[][]interface{}{
			[]interface{}{int64(1), int64(2), int64(3)},
			[]interface{}{"a", "b", "c"},
			[]interface{}{true, false},
		},
	)

	// map with string keys
	do(nil,
		&Call17{
			Name: "call_case_9",
		},
		[][]interface{}{
			[]interface{}{map[string]interface{}{"key1": "value1", "key2": "value2"}},
		},
	)
}

func BenchmarkCall17Pack(b *testing.B) {
	buf := make([]byte, 0)
	for i := 0; i < b.N; i++ {
		buf, _ = (&Call17{Name: "sel_all"}).MarshalMsg(buf[:0])
	}
}
