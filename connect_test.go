package tnt

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnect(t *testing.T) {
	assert := assert.New(t)

	box, err := NewBox("", nil)
	assert.NoError(err)
	defer box.Close()

	conn, err := Connect(fmt.Sprintf("127.0.0.1:%d", box.Port), nil)
	if !assert.NoError(err) {
		return
	}
	defer conn.Close()

	assert.Contains(string(conn.Greeting.Version), "Tarantool")
}

func TestConnectAuth(t *testing.T) {
	assert := assert.New(t)

	box, err := NewBox("", nil)
	assert.NoError(err)
	defer box.Close()

	conn, err := Connect(fmt.Sprintf("127.0.0.1:%d", box.Port), &Options{
		User:     "admin",
		Password: "qwerty",
	})
	if !assert.NoError(err) {
		return
	}
	defer conn.Close()

	assert.Contains(string(conn.Greeting.Version), "Tarantool")
}
