package tarantool

import (
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
	buf := make([]byte, 0)
	buf, err := q.PackMsg(nil, buf)
	require.NoError(t, err)

	qa := &Eval{}
	err = qa.UnmarshalBinary(buf)
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

	res, err := tnt.Execute(q)
	require.NoError(err)
	require.Len(res, 5)
	assert.EqualValues(box.Listen, res[0][0])
	assert.EqualValues(user, res[1][0])
	assert.EqualValues(args[0], res[2][0])
	assert.EqualValues(args[1], res[3][0])
	assert.EqualValues(args, res[4])
}

func BenchmarkEvalPack(b *testing.B) {
	d := newPackData(42)
	buf := make([]byte, 0)
	for i := 0; i < b.N; i++ {
		buf, _ = (&Eval{Expression: "return 2+2"}).PackMsg(d, buf)
	}
}
