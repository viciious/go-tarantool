package tarantool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnect(t *testing.T) {
	assert := assert.New(t)

	box, err := NewBox("", nil)
	assert.NoError(err)
	defer box.Close()

	conn, err := Connect(box.Addr(), nil)
	if !assert.NoError(err) {
		return
	}
	defer conn.Close()

	assert.Contains(string(conn.Greeting.Version), "Tarantool")
}
