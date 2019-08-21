package tarantool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAuth(t *testing.T) {
	assert := assert.New(t)

	tarantoolConfig := `
    box.schema.user.create("tester", {password = "12345678"})
    `

	box, err := NewBox(tarantoolConfig, nil)
	if !assert.NoError(err) {
		return
	}

	defer box.Close()

	// unkwnown user
	conn, err := box.Connect(&Options{
		User:     "user_not_found",
		Password: "qwerty",
	})
	if assert.Error(err) && assert.Nil(conn) {
		assert.Contains(err.Error(), "is not found")
	}

	// bad password
	conn, err = box.Connect(&Options{
		User:     "tester",
		Password: "qwerty",
	})
	if assert.Error(err) && assert.Nil(conn) {
		assert.Contains(err.Error(), "Incorrect password supplied for user")
	}

	// ok user password
	conn, err = box.Connect(&Options{
		User:     "tester",
		Password: "12345678",
	})
	if assert.NoError(err) && assert.NotNil(conn) {
		conn.Close()
	}

}
