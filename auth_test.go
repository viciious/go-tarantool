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

	// unknown user
	conn, err := box.Connect(&Options{
		User:     "user_not_found",
		Password: "qwerty",
	})
	ver, _ := tntBoxVersion(box)

	if assert.Error(err) && assert.Nil(conn) {
		if ver >= version2_11_0 {
			assert.Exactly(err.Error(), "User not found or supplied credentials are invalid")
		} else {
			assert.Contains(err.Error(), "is not found")
		}
	}

	// bad password
	conn, err = box.Connect(&Options{
		User:     "tester",
		Password: "qwerty",
	})
	ver, _ = tntBoxVersion(box)

	if assert.Error(err) && assert.Nil(conn) {
		if ver >= version2_11_0 {
			assert.Exactly(err.Error(), "User not found or supplied credentials are invalid")
		} else {
			assert.Contains(err.Error(), "Incorrect password supplied for user")
		}
	}

	// ok user password
	conn, err = box.Connect(&Options{
		User:     "tester",
		Password: "12345678",
	})
	if assert.NoError(err) && assert.NotNil(conn) {
		assert.NotEmpty(conn.InstanceUUID(), "instance UUID is empty")
		conn.Close()
	}

}
