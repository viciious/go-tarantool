package tarantool

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const lastSnapLSNFile = "tarantool_lastsnaplsn.lua"

func schemeGrantLastSnapLSN(username string) string {
	scheme := `
	dofile('{filename}')
	box.schema.func.create('lastsnaplsn', {if_not_exists = true})
	box.schema.user.grant('{username}', 'execute', 'function', 'lastsnaplsn', {if_not_exists = true})
	`
	scheme = strings.Replace(scheme, "{filename}", lastSnapLSNFile, -1)
	scheme = strings.Replace(scheme, "{username}", username, -1)
	return scheme
}

func TestCallLastSnapLSN(t *testing.T) {
	require := require.New(t)

	user := "guest"
	dirLUA := "lua"
	config := schemeGrantLastSnapLSN(user)
	config += schemeGrantEval(user)

	box, err := NewBox(config, &BoxOptions{WorkDir: dirLUA})
	require.NoError(err)
	defer box.Close()

	tnt, err := Connect(box.Listen, &Options{})
	require.NoError(err)
	defer tnt.Close()

	// prepare another one snapshot
	makesnapshot := &Eval{Expression: "local box = require('box') box.snapshot()"}

	res, err := tnt.Execute(makesnapshot)
	require.NoError(err)
	require.Len(res, 0, "response to make snapshot request contains error")

	// get newly generated snapshot LSN
	lastsnaplsn := &Call{Name: "lastsnaplsn"}
	res, err = tnt.Execute(lastsnaplsn)
	require.NoError(err)
	require.NotEmpty(res, "result [][]interface is empty")
	require.NotEmpty(res[0], "result[0] []interface is empty")
	switch res := res[0][0].(type) {
	case uint64:
		require.True(res > 0, "newly generated snapshot should have LSN greater than zero")
		t.Logf("Last snapshot LSN: %v", res)
	default:
		t.Fatalf("NaN LSN:%#v", res)
	}
}
