package snapio

import (
	"os"
	"path/filepath"
	"testing"
)

func checkSnapshotCnt(v, fn string, expected int, t *testing.T) {
	ffn := filepath.Join("testdata", v, fn)
	f, e := os.Open(ffn)
	if e != nil {
		t.Error(e)
		return
	}
	defer f.Close()

	cnt := 0
	e = ReadSnapshot(f, func(space uint, tuple []interface{}) {
		cnt++
	})

	if e != nil {
		t.Error(e)
		return
	}

	if cnt != expected {
		t.Errorf("%s: cnt == %d, expected %d", ffn, cnt, expected)
	}
}

func TestReadv12OK(t *testing.T) {
	checkSnapshotCnt("v12", "00000000000000000000.ok.snap", 62, t)
}

func TestReadv13OK(t *testing.T) {
	checkSnapshotCnt("v13", "00000000000000010005.ok.snap", 10511, t)
}
