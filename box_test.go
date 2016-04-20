package tarantool

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBox(t *testing.T) {
	if os.Getenv("TARANTOOL16") == "" {
		t.Skip("skipping tarantool16 tests")
	}
	assert := assert.New(t)

	config := `
    box.info()
    `

	box, err := NewBox(config, &BoxOptions{})
	assert.NoError(err)
	defer box.Close()

}
