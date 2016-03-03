package tnt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBox(t *testing.T) {
	assert := assert.New(t)

	config := `
    box.info()
    `

	box, err := NewBox(config, &BoxOptions{})

	assert.NoError(err)
	defer box.Close()

}
