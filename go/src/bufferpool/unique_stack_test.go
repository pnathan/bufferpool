package bufferpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHappyPathUniqueStack(t *testing.T) {
	// Can we create this?
	us := NewUniqueStack[int]()
	assert.Equal(t, us.Length(), 0)
	us.Push(10)
	assert.Equal(t, us.Length(), 1)
	assert.Equal(t, us.Top(), 10)
	assert.Equal(t, us.Bottom(), 10)

	us.Push(20)
	assert.Equal(t, us.Length(), 2)
	assert.Equal(t, us.Top(), 20)
	assert.Equal(t, us.Bottom(), 10)

	// uniqueness/reprioritization check
	us.Push(10)
	assert.Equal(t, us.Length(), 2)
	assert.Equal(t, us.Top(), 10)
	assert.Equal(t, us.Bottom(), 20)

	us.Push(30)
	assert.Equal(t, us.Length(), 3)
	assert.Equal(t, us.Top(), 30)
	assert.Equal(t, us.Bottom(), 20)

	w := us.Pop()
	assert.Equal(t, w, 30)
	assert.Equal(t, us.Length(), 2)

	x := us.Pop()
	assert.Equal(t, x, 10)
	assert.Equal(t, us.Length(), 1)

	y := us.Pop()
	assert.Equal(t, y, 20)
	assert.Equal(t, us.Length(), 0)

	assert.Equal(t, us.Length(), 0)
	us.Push(100)
	assert.Equal(t, us.Length(), 1)
}
