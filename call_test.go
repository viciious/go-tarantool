package tarantool

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCall(t *testing.T) {
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
        return
    end

    local number_of_extra_cases = 3

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

	type testParams struct {
		query           *Call
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
			var query2 = &Call{}
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

		if assert.NoError(res.Error) {
			assert.Equal(params.expectedData, res.Data)
			assert.Equal(params.expectedRawData, res.RawData)
		}
	}

	// call sel_all without params
	do(&testParams{
		query: &Call{
			Name: "sel_all",
		},
		expectedData: [][]interface{}{
			{int64(1), "First record"},
			{int64(2), "Music"},
			{int64(3), "Length", int64(93)},
		},
	})
	do(&testParams{
		query: &Call{
			Name: "sel_all",
		},
		execOption: ExecResultAsDataWithFallback,
		expectedData: [][]interface{}{
			{int64(1), "First record"},
			{int64(2), "Music"},
			{int64(3), "Length", int64(93)},
		},
	})
	do(&testParams{
		query: &Call{
			Name: "sel_all",
		},
		execOption: ExecResultAsRawData,
		expectedRawData: []interface{}{
			[]interface{}{int64(1), "First record"},
			[]interface{}{int64(2), "Music"},
			[]interface{}{int64(3), "Length", int64(93)},
		},
	})

	// call sel_name with params
	do(&testParams{
		query: &Call{
			Name:  "sel_name",
			Tuple: []interface{}{int64(2), "Music"},
		},
		expectedData: [][]interface{}{
			{int64(2), "Music"},
		},
	})
	do(&testParams{
		query: &Call{
			Name:  "sel_name",
			Tuple: []interface{}{int64(2), "Music"},
		},
		execOption: ExecResultAsDataWithFallback,
		expectedData: [][]interface{}{
			{int64(2), "Music"},
		},
	})
	do(&testParams{
		query: &Call{
			Name:  "sel_name",
			Tuple: []interface{}{int64(2), "Music"},
		},
		execOption: ExecResultAsRawData,
		expectedRawData: []interface{}{
			[]interface{}{int64(2), "Music"},
		},
	})

	// scalar 1
	do(&testParams{
		query: &Call{
			Name: "call_case_1",
		},
		expectedData: [][]interface{}{
			{int64(1)},
		},
	})
	do(&testParams{
		query: &Call{
			Name: "call_case_1",
		},
		execOption: ExecResultAsDataWithFallback,
		expectedData: [][]interface{}{
			{int64(1)},
		},
	})
	do(&testParams{
		query: &Call{
			Name: "call_case_1",
		},
		execOption: ExecResultAsRawData,
		expectedRawData: []interface{}{
			[]interface{}{int64(1)},
		},
	})

	// multiple scalars
	do(&testParams{
		query: &Call{
			Name: "call_case_2",
		},
		expectedData: [][]interface{}{
			{int64(1)}, {int64(2)}, {int64(3)},
		},
	})
	do(&testParams{
		query: &Call{
			Name: "call_case_2",
		},
		execOption: ExecResultAsDataWithFallback,
		expectedData: [][]interface{}{
			{int64(1)}, {int64(2)}, {int64(3)},
		},
	})
	do(&testParams{
		query: &Call{
			Name: "call_case_2",
		},
		execOption: ExecResultAsRawData,
		expectedRawData: []interface{}{
			[]interface{}{int64(1)},
			[]interface{}{int64(2)},
			[]interface{}{int64(3)},
		},
	})

	// empty result
	do(&testParams{
		query: &Call{
			Name: "call_case_3",
		},
		expectedData: [][]interface{}{},
	})
	do(&testParams{
		query: &Call{
			Name: "call_case_3",
		},
		execOption:   ExecResultAsDataWithFallback,
		expectedData: [][]interface{}{},
	})
	do(&testParams{
		query: &Call{
			Name: "call_case_3",
		},
		execOption:      ExecResultAsRawData,
		expectedRawData: []interface{}{},
	})
}

func BenchmarkCallPack(b *testing.B) {
	buf := make([]byte, 0)
	for i := 0; i < b.N; i++ {
		buf, _ = (&Call{Name: "sel_all"}).MarshalMsg(buf[:0])
	}
}
