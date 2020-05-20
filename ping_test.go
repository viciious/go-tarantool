package tarantool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPing(t *testing.T) {
	assert := assert.New(t)

	tarantoolConfig := `
    local s = box.schema.space.create('tester')
    `

	box, err := NewBox(tarantoolConfig, nil)
	if !assert.NoError(err) {
		return
	}
	defer box.Close()

	conn, err := box.Connect(nil)
	assert.NoError(err)
	assert.NotNil(conn)

	defer conn.Close()

	data, err := conn.Execute(&Ping{})
	assert.NoError(err)
	assert.Nil(data)
}
