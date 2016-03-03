package tnt

import (
	"testing"
	"time"

	"github.com/k0kubun/pp"
	"github.com/stretchr/testify/assert"
)

func TestSelect(t *testing.T) {
	assert := assert.New(t)

	tarantoolConfig := `
    s = box.schema.space.create('tester')
    s:create_index('primary', {
        type = 'hash',
        parts = {1, 'NUM'}
    })
    t = s:insert({1})
    t = s:insert({2, 'Music'})
    t = s:insert({3, 'Length', 93})
    `

	box, err := NewBox(tarantoolConfig, nil)
	if !assert.NoError(err) {
		return
	}
	defer box.Close()

	// unkwnown user
	conn, err := box.Connect(nil)
	assert.NoError(err)
	assert.NotNil(conn)

	pp.Println(conn.Execute(&Select{
		Space: "tester",
		Key:   Uint32(3),
	}))

	time.Sleep(time.Second)

}
