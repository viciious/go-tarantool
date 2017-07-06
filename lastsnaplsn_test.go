package tarantool

import (
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func schemeGrantLastSnapLSN(user, pass string) string {
	scheme := `
	box.once('{user}:role_replication', function()
		box.schema.user.create('{user}', {password = '{pass}', if_not_exists = true})
		box.schema.user.grant('{user}','execute','function','lastsnaplsn', {if_not_exists = true})
	end)
	`
	scheme = strings.Replace(scheme, "{user}", user, -1)
	scheme = strings.Replace(scheme, "{pass}", pass, -1)
	return scheme
}

func schemeGrantRoleLastSnapLSN(role string) string {
	scheme := `
	box.once('{role}:exec_lastsnaplsn', function()
		box.schema.role.grant('{role}', 'execute', 'function', 'lastsnaplsn', {if_not_exists = true})
	end)
	`
	scheme = strings.Replace(scheme, "{role}", role, -1)
	return scheme
}

// TestCallLastSnapLSN test grants for call lastsnaplsn procedure:
// 1) direct execute grant on lastsnaplsn func
// 2) grant on replication role
func TestCallLastSnapLSN(t *testing.T) {
	require := require.New(t)

	guest, luaDir, role := "guest", "lua", "replication"
	luaInit, err := ioutil.ReadFile(filepath.Join(luaDir, "init.lua"))
	require.NoError(err)
	config := string(luaInit)
	config += schemeGrantLastSnapLSN(tnt16User+"1", tnt16Pass)
	config += schemeGrantRoleLastSnapLSN(role)
	config += schemeNewReplicator(tnt16User+"2", tnt16Pass)
	config += schemeGrantEval(guest)

	box, err := NewBox(config, &BoxOptions{WorkDir: luaDir})
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
	tnt.Close()

	lastsnaplsn := &Call{Name: "lastsnaplsn"}
	tt := []struct {
		user string
		pass string
	}{
		{tnt16User + "1", tnt16Pass},
		{tnt16User + "2", tnt16Pass},
	}
	for tc, item := range tt {
		// get newly generated snapshot LSN
		tnt, err = Connect(box.Listen, &Options{User: item.user, Password: item.pass})
		require.NoError(err, "case %v (connect)", tc)
		res, err = tnt.Execute(lastsnaplsn)
		require.NoError(err, "case %v (exec)", tc)
		require.NotEmpty(res, "result [][]interface is empty (%v)", tc)
		require.NotEmpty(res[0], "result[0] []interface is empty (%v)", tc)
		switch res := res[0][0].(type) {
		case uint64:
			require.True(res > 0, "newly generated snapshot should have LSN greater than zero (%v)", tc)
			t.Logf("Last snapshot LSN: %v", res)
		default:
			t.Fatalf("NaN LSN:%#v (%v)", res, tc)
		}
	}
}
