package tarantool

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLastSnapVClock(t *testing.T) {
	require := require.New(t)

	guest, role, luaDir := "guest", "replication", "lua"
	luaInit, err := ioutil.ReadFile(filepath.Join("testdata", "init.lua"))
	require.NoError(err)
	config := string(luaInit)
	config += schemeNewReplicator(tnt16User, tnt16Pass)
	config += schemeGrantRoleFunc(role, procLUALastSnapVClock)
	config += schemeGrantRoleFunc(role, "readfile")
	config += schemeGrantRoleFunc(role, "lastsnapfilename")
	config += schemeGrantRoleFunc(role, "parsevclock")
	// for making snapshot
	config += schemeGrantUserEval(guest)

	box, err := NewBox(config, &BoxOptions{WorkDir: luaDir})
	require.NoError(err)
	defer box.Close()

	// add replica to replica set
	s, err := NewSlave(box.Listen, Options{User: tnt16User, Password: tnt16Pass})
	require.NoError(err)
	defer s.Close()
	if s.Version() > version1_7_0 {
		t.Skip("LastSnapVClock is depricated for tarantools above 1.7.0")
	}
	err = s.Join()
	require.NoError(err)

	// make snapshot
	tnt, err := Connect(box.Listen, &Options{})
	require.NoError(err)
	defer tnt.Close()
	makesnapshot := &Eval{Expression: luaMakeSnapshot}
	res, err := tnt.Execute(makesnapshot)
	require.NoError(err)
	require.Empty(res, 0, "response to make snapshot request contains error")
	tnt.Close()

	// test each lua func separately
	t.Run(procLUALastSnapVClock, SubTestVClockSelf(box))
	t.Run("readfile", SubTestVClockReadFile(box))
	t.Run("lastsnapfilename", SubTestVClockLastSnapFilename(box))
	t.Run("parsevclock", SubTestVClockParseVClock(box))
}

func SubTestVClockSelf(box *Box) func(t *testing.T) {
	return func(t *testing.T) {
		lastsnapvclock := &Call{Name: procLUALastSnapVClock}
		tnt, err := Connect(box.Listen, &Options{User: tnt16User, Password: tnt16Pass})
		require.NoError(t, err, "connect")
		res, err := tnt.Execute(lastsnapvclock)
		require.NoError(t, err, "exec")
		require.NotEmpty(t, res, "result [][]interface is empty")
		require.Len(t, res[0], 2, "vector clock should contain two clocks")
		switch res := res[0][0].(type) {
		case int64:
			require.True(t, res > 0, "master clock (result[0][0]) should be greater than zero")
		default:
			t.Fatalf("NaN master clock: %#v (%T)", res, res)
		}
		switch res := res[0][1].(type) {
		case int64:
			require.True(t, res == 0, "replica clock (result[0][1]) should be zero")
		default:
			t.Fatalf("NaN master clock: %#v (%T)", res, res)
		}
	}
}

func SubTestVClockReadFile(box *Box) func(t *testing.T) {
	return func(t *testing.T) {
		tnt, err := Connect(box.Listen, &Options{User: tnt16User, Password: tnt16Pass})
		require.NoError(t, err, "connect")
		luaProc := &Call{Name: "readfile"}

		luaProc.Tuple = []interface{}{"notexist.snap", 255}
		res, err := tnt.Execute(luaProc)
		require.NoError(t, err, "exec")

		require.Len(t, res, 2, "should be data tuple and error tuple in result")
		require.NotEmpty(t, res[0], "data tuple")
		require.Nil(t, res[0][0], "data should be nil")
		require.NotEmpty(t, res[1], "error tuple")
		require.Contains(t, res[1][0], "such file", "err should be about file")

		luaProc.Tuple = []interface{}{"tarantool_lastsnapvclock.lua", 255}
		res, err = tnt.Execute(luaProc)
		require.NoError(t, err, "exec")

		require.Len(t, res, 2, "should be data tuple and error tuple in result")
		require.NotEmpty(t, res[0], "data tuple")
		require.Contains(t, res[0][0], "VERSION", "data should contain vclock")
		require.NotEmpty(t, res[1], "error tuple")
		require.Nil(t, res[1][0], "err should be nil")
	}
}

func SubTestVClockLastSnapFilename(box *Box) func(t *testing.T) {
	return func(t *testing.T) {
		tnt, err := Connect(box.Listen, &Options{User: tnt16User, Password: tnt16Pass})
		require.NoError(t, err, "connect")
		luaProc := &Call{Name: "lastsnapfilename"}

		res, err := tnt.Execute(luaProc)
		require.NoError(t, err, "exec")

		require.NotEmpty(t, res)
		require.NotEmpty(t, res[0])
		require.IsType(t, "", res[0][0])
		realsnap := res[0][0].(string)
		t.Logf("Real snapshot: %v", realsnap)

		fakesnapfile, err := os.Create(filepath.Join(box.Root, "snap", "10000000000000000000.snap"))
		require.NoError(t, err)
		fakesnapfile.Close()
		defer os.Remove(fakesnapfile.Name())
		t.Logf("Create fake snapshot: %v", fakesnapfile.Name())

		res, err = tnt.Execute(luaProc)
		require.NoError(t, err, "exec")

		require.NotEmpty(t, res)
		require.NotEmpty(t, res[0])
		require.IsType(t, "", res[0][0])
		fakesnap := res[0][0].(string)
		t.Logf("Fake snapshot: %v", fakesnap)

		require.Equal(t, fakesnapfile.Name(), fakesnap)
		require.NotEqual(t, realsnap, fakesnap)
	}
}

func SubTestVClockParseVClock(box *Box) func(t *testing.T) {
	return func(t *testing.T) {
		tnt, err := Connect(box.Listen, &Options{User: tnt16User, Password: tnt16Pass})
		require.NoError(t, err, "connect")
		luaProc := &Call{Name: "parsevclock"}

		tt := []struct {
			str string
			vc  []interface{}
		}{
			// failed to parse -> nil result
			{"VClock:{}", []interface{}{interface{}(nil)}},
			// parse empty Vlock -> empty slice
			{"VClock: {}", []interface{}{}},
			{"VClock: {1:10}", []interface{}{int64(10)}},
			{"VClock: {1:10, 2:0}", []interface{}{int64(10), int64(0)}},
			{"VClock: { 1:10,2:0 }", []interface{}{int64(10), int64(0)}},
		}
		for tc, item := range tt {
			luaProc.Tuple = []interface{}{item.str}
			res, err := tnt.Execute(luaProc)
			require.NoError(t, err, "case %v (exec)", tc+1)
			require.NotEmpty(t, res, "case %v (result array)", tc+1)
			assert.Equal(t, item.vc, res[0])
		}
	}
}
