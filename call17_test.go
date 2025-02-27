package tarantool

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCall17(t *testing.T) {
	assert := assert.New(t)

	tarantoolConfig := `
    local s = box.schema.space.create('tester', {id = 42})
    s:create_index('tester_id', {
        type = 'tree',
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
        return box.space.tester:select({}, {iterator = "ALL"})
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

    function call_case_10()
        return
    end

    local number_of_extra_cases = 10

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
		t.Skip("requires tarantool >= 1.7.2")
	}

	type testParams struct {
		query           *Call17
		execOption      ExecOption
		expectedData    [][]interface{}
		expectedRawData interface{}
	}

	do := func(params *testParams) {
		var buf []byte

		conn, err := box.Connect(nil)
		assert.NoError(err)
		assert.NotNil(conn)

		defer conn.Close()

		buf, err = params.query.MarshalMsg(nil)

		if assert.NoError(err) {
			var query2 = &Call17{}
			_, err = query2.UnmarshalMsg(buf)

			if assert.NoError(err) {
				assert.Equal(params.query.Name, query2.Name)
				assert.Equal(params.query.Tuple, query2.Tuple)
			}
		}

		var opts []ExecOption
		if params.execOption != nil {
			opts = append(opts, params.execOption)
		}
		res := conn.Exec(context.Background(), params.query, opts...)

		if assert.NoError(err) {
			assert.Equal(params.expectedData, res.Data)
			assert.Equal(params.expectedRawData, res.RawData)
		}
	}

	// call sel_all without params
	do(&testParams{
		query: &Call17{
			Name: "sel_all",
		},
		expectedData: [][]interface{}{
			{
				[]interface{}{int64(1), "First record"},
				[]interface{}{int64(2), "Music"},
				[]interface{}{int64(3), "Length", int64(93)},
			},
		},
	})
	do(&testParams{
		query: &Call17{
			Name: "sel_all",
		},
		execOption: ExecResultAsDataWithFallback,
		expectedData: [][]interface{}{
			{
				[]interface{}{int64(1), "First record"},
				[]interface{}{int64(2), "Music"},
				[]interface{}{int64(3), "Length", int64(93)},
			},
		},
	})
	do(&testParams{
		query: &Call17{
			Name: "sel_all",
		},
		execOption: ExecResultAsRawData,
		expectedRawData: []interface{}{
			[]interface{}{
				[]interface{}{int64(1), "First record"},
				[]interface{}{int64(2), "Music"},
				[]interface{}{int64(3), "Length", int64(93)},
			},
		},
	})

	// call sel_name with params
	do(&testParams{
		query: &Call17{
			Name:  "sel_name",
			Tuple: []interface{}{int64(2), "Music"},
		},
		expectedData: [][]interface{}{
			{
				[]interface{}{int64(2), "Music"},
			},
		},
	})
	do(&testParams{
		query: &Call17{
			Name:  "sel_name",
			Tuple: []interface{}{int64(2), "Music"},
		},
		execOption: ExecResultAsDataWithFallback,
		expectedData: [][]interface{}{
			{
				[]interface{}{int64(2), "Music"},
			},
		},
	})
	do(&testParams{
		query: &Call17{
			Name:  "sel_name",
			Tuple: []interface{}{int64(2), "Music"},
		},
		execOption: ExecResultAsRawData,
		expectedRawData: []interface{}{
			[]interface{}{
				[]interface{}{int64(2), "Music"},
			},
		},
	})

	// For stored procedures the result is returned in the same way as eval (in certain cases).
	// Note that returning arrays (also an empty table) is a special case.

	// scalar 1
	do(&testParams{
		query: &Call17{
			Name: "call_case_1",
		},
		expectedData: [][]interface{}{
			{int64(1)},
		},
	})
	do(&testParams{
		query: &Call17{
			Name: "call_case_1",
		},
		execOption:      ExecResultAsDataWithFallback,
		expectedRawData: []interface{}{int64(1)},
	})
	do(&testParams{
		query: &Call17{
			Name: "call_case_1",
		},
		execOption:      ExecResultAsRawData,
		expectedRawData: []interface{}{int64(1)},
	})

	// multiple scalars
	do(&testParams{
		query: &Call17{
			Name: "call_case_2",
		},
		expectedData: [][]interface{}{
			{int64(1)}, {int64(2)}, {int64(3)},
		},
	})
	do(&testParams{
		query: &Call17{
			Name: "call_case_2",
		},
		execOption: ExecResultAsDataWithFallback,
		expectedRawData: []interface{}{
			int64(1), int64(2), int64(3),
		},
	})
	do(&testParams{
		query: &Call17{
			Name: "call_case_2",
		},
		execOption: ExecResultAsRawData,
		expectedRawData: []interface{}{
			int64(1), int64(2), int64(3),
		},
	})

	// scalar true
	do(&testParams{
		query: &Call17{
			Name: "call_case_3",
		},
		expectedData: [][]interface{}{
			{true},
		},
	})
	do(&testParams{
		query: &Call17{
			Name: "call_case_3",
		},
		execOption:      ExecResultAsDataWithFallback,
		expectedRawData: []interface{}{true},
	})
	do(&testParams{
		query: &Call17{
			Name: "call_case_3",
		},
		execOption:      ExecResultAsRawData,
		expectedRawData: []interface{}{true},
	})

	// scalar nil
	do(&testParams{
		query: &Call17{
			Name: "call_case_4",
		},
		expectedData: [][]interface{}{
			{nil},
		},
	})
	do(&testParams{
		query: &Call17{
			Name: "call_case_4",
		},
		execOption: ExecResultAsDataWithFallback,
		expectedRawData: []interface{}{
			interface{}(nil),
		},
	})
	do(&testParams{
		query: &Call17{
			Name: "call_case_4",
		},
		execOption: ExecResultAsRawData,
		expectedRawData: []interface{}{
			interface{}(nil),
		},
	})

	// empty table
	do(&testParams{
		query: &Call17{
			Name: "call_case_5",
		},
		expectedData: [][]interface{}{
			{},
		},
	})
	do(&testParams{
		query: &Call17{
			Name: "call_case_5",
		},
		execOption: ExecResultAsDataWithFallback,
		expectedData: [][]interface{}{
			{},
		},
	})
	do(&testParams{
		query: &Call17{
			Name: "call_case_5",
		},
		execOption: ExecResultAsRawData,
		expectedRawData: []interface{}{
			[]interface{}{},
		},
	})

	// array with len 1 (similar to case 1)
	do(&testParams{
		query: &Call17{
			Name: "call_case_6",
		},
		expectedData: [][]interface{}{
			{int64(1)},
		},
	})
	do(&testParams{
		query: &Call17{
			Name: "call_case_6",
		},
		execOption: ExecResultAsDataWithFallback,
		expectedData: [][]interface{}{
			{int64(1)},
		},
	})
	do(&testParams{
		query: &Call17{
			Name: "call_case_6",
		},
		execOption: ExecResultAsRawData,
		expectedRawData: []interface{}{
			[]interface{}{int64(1)},
		},
	})

	// single array with len 3
	do(&testParams{
		query: &Call17{
			Name: "call_case_7",
		},
		expectedData: [][]interface{}{
			{int64(1), int64(2), int64(3)},
		},
	})
	do(&testParams{
		query: &Call17{
			Name: "call_case_7",
		},
		execOption: ExecResultAsDataWithFallback,
		expectedData: [][]interface{}{
			{int64(1), int64(2), int64(3)},
		},
	})
	do(&testParams{
		query: &Call17{
			Name: "call_case_7",
		},
		execOption: ExecResultAsRawData,
		expectedRawData: []interface{}{
			[]interface{}{int64(1), int64(2), int64(3)},
		},
	})

	// multiple arrays
	do(&testParams{
		query: &Call17{
			Name: "call_case_8",
		},
		expectedData: [][]interface{}{
			{int64(1), int64(2), int64(3)},
			{"a", "b", "c"},
			{true, false},
		},
	})
	do(&testParams{
		query: &Call17{
			Name: "call_case_8",
		},
		execOption: ExecResultAsDataWithFallback,
		expectedData: [][]interface{}{
			{int64(1), int64(2), int64(3)},
			{"a", "b", "c"},
			{true, false},
		},
	})
	do(&testParams{
		query: &Call17{
			Name: "call_case_8",
		},
		execOption: ExecResultAsRawData,
		expectedRawData: []interface{}{
			[]interface{}{int64(1), int64(2), int64(3)},
			[]interface{}{"a", "b", "c"},
			[]interface{}{true, false},
		},
	})

	// map with string keys
	do(&testParams{
		query: &Call17{
			Name: "call_case_9",
		},
		expectedData: [][]interface{}{
			{map[string]interface{}{"key1": "value1", "key2": "value2"}},
		},
	})
	do(&testParams{
		query: &Call17{
			Name: "call_case_9",
		},
		execOption: ExecResultAsDataWithFallback,
		expectedRawData: []interface{}{
			map[string]interface{}{"key1": "value1", "key2": "value2"},
		},
	})
	do(&testParams{
		query: &Call17{
			Name: "call_case_9",
		},
		execOption: ExecResultAsRawData,
		expectedRawData: []interface{}{
			map[string]interface{}{"key1": "value1", "key2": "value2"},
		},
	})

	// empty result
	do(&testParams{
		query: &Call17{
			Name: "call_case_10",
		},
		expectedData: [][]interface{}{},
	})
	do(&testParams{
		query: &Call17{
			Name: "call_case_10",
		},
		execOption:   ExecResultAsDataWithFallback,
		expectedData: [][]interface{}{},
	})
	do(&testParams{
		query: &Call17{
			Name: "call_case_10",
		},
		execOption:      ExecResultAsRawData,
		expectedRawData: []interface{}{},
	})
}

func BenchmarkCall17Pack(b *testing.B) {
	buf := make([]byte, 0)
	for i := 0; i < b.N; i++ {
		buf, _ = (&Call17{Name: "sel_all"}).MarshalMsg(buf[:0])
	}
}
