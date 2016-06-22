package tarantool

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	if os.Getenv("TARANTOOL16") != "" {
		os.Exit(m.Run())
	}
}
