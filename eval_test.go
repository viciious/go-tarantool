package tarantool

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func schemeGrantUserEval(username string) string {
	scheme := `
    box.schema.user.grant('{username}', 'execute', 'universe')
    `
	return strings.Replace(scheme, "{username}", username, -1)
}

func TestEvalPackUnpack(t *testing.T) {
	q := &Eval{Expression: "return 2+2", Tuple: []interface{}{"test"}}
	// check unpack
	buf, err := q.MarshalMsg(nil)
	require.NoError(t, err)

	qa := &Eval{}
	_, err = qa.UnmarshalMsg(buf)
	require.NoError(t, err)
	assert.Equal(t, q, qa)
}

func TestEvalExecute(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	user := "guest"
	config := schemeGrantUserEval(user)
	expr := "local arg = {...} return box.cfg.listen, box.session.user(), arg[1], arg[2], arg"
	args := []interface{}{"one", "two"}
	q := &Eval{Expression: expr, Tuple: args}

	box, err := NewBox(config, &BoxOptions{})
	require.NoError(err)
	defer box.Close()

	tnt, err := Connect(box.Listen, &Options{})
	require.NoError(err)

	data, err := tnt.Execute(q)
	require.NoError(err)
	require.Len(data, 5)
	assert.EqualValues(box.Listen, data[0][0])
	assert.EqualValues(user, data[1][0])
	assert.EqualValues(args[0], data[2][0])
	assert.EqualValues(args[1], data[3][0])
	assert.EqualValues(args, data[4])

	res := tnt.Exec(context.Background(), q, ExecResultAsDataWithFallback)
	require.NoError(res.Error)
	require.Nil(res.Data)
	assert.Equal(res.RawData, []interface{}{
		box.Listen, user, args[0], args[1], args,
	})
}

func BenchmarkEvalPack(b *testing.B) {
	buf := make([]byte, 0)
	for i := 0; i < b.N; i++ {
		buf, _ = (&Eval{Expression: "return 2+2"}).MarshalMsg(buf[:0])
	}
}
