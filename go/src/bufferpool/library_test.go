package bufferpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleFramePool(t *testing.T) {
	x := NewMockPool(0)
	assert.Equal(t, x.Size(), 0)
}
