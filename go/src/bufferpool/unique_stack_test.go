package bufferpool

import (
	"fmt"
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

func TestUniqueStack_Delete(t *testing.T) {

	type testCase[K comparable] struct {
		name    string
		o       *UniqueStack[K]
		args    K
		wantErr assert.ErrorAssertionFunc
	}
	tests := []testCase[int]{
		{
			name:    "Empty",
			o:       NewUniqueStack[int](),
			args:    1,
			wantErr: assert.Error,
		},
		{
			name:    "One",
			o:       NewUniqueStack[int]().With(1),
			args:    1,
			wantErr: assert.NoError,
		},
		{
			name:    "One and doesn't have",
			o:       NewUniqueStack[int]().With(2),
			args:    1,
			wantErr: assert.Error,
		},
		{
			name:    "Two and has",
			o:       NewUniqueStack[int]().With(1).With(2),
			args:    2,
			wantErr: assert.NoError,
		},
		{
			name:    "Two and doesn't have",
			o:       NewUniqueStack[int]().With(1).With(2).With(3).With(4).With(5),
			args:    -1,
			wantErr: assert.Error,
		},
		{
			name:    "Five",
			o:       NewUniqueStack[int]().With(1).With(2).With(3).With(4).With(5),
			args:    5,
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, tt.o.Delete(tt.args), fmt.Sprintf("Delete(%v)", tt.args))
		})
	}
}
