package tnt

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnectAuth(t *testing.T) {
	assert := assert.New(t)

	tarantoolConfig := `
    box.schema.user.create("tester", {password = "12345678"})
    `

	box, err := NewBox(tarantoolConfig, nil)
	assert.NoError(err)
	defer box.Close()

	// unkwnown user
	conn, err := Connect(fmt.Sprintf("127.0.0.1:%d", box.Port), &Options{
		User:     "user_not_found",
		Password: "qwerty",
	})
	if assert.Error(err) && assert.Nil(conn) {
		assert.Contains(err.Error(), "is not found")
	}

	// bad password
	conn, err = Connect(fmt.Sprintf("127.0.0.1:%d", box.Port), &Options{
		User:     "tester",
		Password: "qwerty",
	})
	if assert.Error(err) && assert.Nil(conn) {
		assert.Contains(err.Error(), "Incorrect password supplied for user")
	}

	// ok user password
	conn, err = Connect(fmt.Sprintf("127.0.0.1:%d", box.Port), &Options{
		User:     "tester",
		Password: "12345678",
	})
	if assert.NoError(err) && assert.NotNil(conn) {
		conn.Close()
	}

}
