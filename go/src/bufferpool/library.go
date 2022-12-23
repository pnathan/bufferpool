package bufferpool

import (
	"sync"
)

// Consider using a Result[T] / Either[L, R] for best use.
// Not idiomatic go, but code probably is nicer.
// "github.com/samber/mo"

type FramePool interface {
	// Review the backing store size
	AssessSize() (int, error)
	// Get the cached size
	Size() int
	ReadFrame(int) (*PageFrame, error)
	WriteFrame(int, *PageFrame) error
	// Increases by int the number of Frame files
	Falloc(int) error
}

type MockPool struct {
	frames map[int]*PageFrame
	size   int
}

func NewMockPool(size int) *MockPool {
	mp := &MockPool{
		frames: map[int]*PageFrame{},
		size:   0,
	}
	if size > 0 {
		mp.Falloc(size)
	}

	return mp

}

func (o *MockPool) AssessSize() (int, error) {
	return len(o.frames), nil
}

func (o *MockPool) Size() int {
	return len(o.frames)
}

func (o *MockPool) ReadFrame(idx int) (*PageFrame, error) {
	return o.frames[idx], nil
}

func (o *MockPool) WriteFrame(idx int, pg *PageFrame) error {
	o.frames[idx] = pg
	return nil
}

func (o *MockPool) Falloc(count int) error {
	prior := o.size
	for i := 0; i < count; i++ {
		pageid := prior + i
		o.WriteFrame(pageid, NewPageFrame([]byte{}))
	}
	o.size += count

	return nil
}

type Evictor interface {
	// This SHOULD be the signature. But Go is brain-damaged.
	//Evict(PageFrame, map[PageFrame]int, UniqueStack[int]) error
	Evict(*PageFrame, map[int]int, *UniqueStack[int]) error
}

type RandomEvictor struct{}

func (o RandomEvictor) Evict(pf *PageFrame, pf_idx map[int]int, lru *UniqueStack[int]) error {
	// todo
	return nil
}

type BottomEvictor struct{}

func (o BottomEvictor) Evict(pf *PageFrame, pf_idx map[int]int, lru *UniqueStack[int]) error {
	// todo
	return nil

}

type DiskPool struct {
}

type PageFrame struct {
	// Does the mutex belong here? probably.
	m     sync.Mutex
	frame []byte
	pins  int
	dirty bool
}

func NewPageFrame(b []byte) *PageFrame {
	return &PageFrame{
		frame: b,
		pins:  0,
		dirty: false,
	}
}

func (o *PageFrame) IncPin() {
	o.m.Lock()
	defer o.m.Unlock()
	o.pins += 1
}

func (o *PageFrame) DecPin() {
	o.m.Lock()
	defer o.m.Unlock()
	o.pins -= 1
}

func (o *PageFrame) Pin() int {
	o.m.Lock()
	defer o.m.Unlock()
	return o.pins
}

func (o *PageFrame) IsDirty() bool {
	o.m.Lock()
	defer o.m.Unlock()
	return o.dirty
}

// WithRead - Pass the data into the passed-in function.  The function
// should not be able to return errors, etc. That is, it should be a
// pure function with respect to the page frame.
func (o *PageFrame) WithRead(f func([]byte)) {

	o.m.Lock()
	defer o.m.Unlock()

	f(o.frame)
}

type BufferPoolId = int

type FramePoolId = int

type BufferPool struct {
}
