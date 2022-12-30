package bufferpool

import (
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
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
	m      sync.RWMutex
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
	o.m.RLock()
	defer o.m.RUnlock()
	return len(o.frames), nil
}

func (o *MockPool) Size() int {
	o.m.RLock()
	defer o.m.RUnlock()
	return len(o.frames)
}

func (o *MockPool) ReadFrame(idx int) (*PageFrame, error) {
	o.m.RLock()
	defer o.m.RUnlock()
	val, ok := o.frames[idx]
	if !ok {
		return nil, fmt.Errorf("%d not in pool", idx)
	}
	return val, nil
}

func (o *MockPool) WriteFrame(idx int, pg *PageFrame) error {
	o.m.Lock()
	defer o.m.Unlock()
	o.frames[idx] = pg
	return nil
}

func (o *MockPool) Falloc(count int) error {
	o.m.Lock()
	defer o.m.Unlock()
	prior := o.size
	for i := 0; i < count; i++ {
		pageid := prior + i
		o.frames[pageid] = NewPageFrame([]byte{})
	}
	o.size += count

	return nil
}

type DiskPool struct {
	loadedFrames map[int]*PageFrame
	knownSize    int
	path         string
	m            sync.RWMutex
}

// NewDiskPool locates the directory path; if the directory doesn't
// exist, error.  Then, Falloc is called on limit, which will ensure
// at least `limit` frames are created. If those frames exist already,
// they are not recreated.
func NewDiskPool(limit int, directoryPath string) (*DiskPool, error) {
	s, err := os.Stat(directoryPath)
	if err != nil {
		return nil, err
	}
	if !s.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", directoryPath)
	}

	dp := &DiskPool{
		loadedFrames: map[int]*PageFrame{},
		knownSize:    0,
		path:         directoryPath,
	}
	err = dp.Falloc(limit)
	if err != nil {
		return nil, err
	}
	return dp, nil
}

func (o *DiskPool) Size() int {
	return o.knownSize
}

func (o *DiskPool) AssessSize() (int, error) {
	files, err := ioutil.ReadDir(o.path)
	if err != nil {
		log.Fatal(err)
	}

	count := 0
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "page_") {
			count++
		}
	}
	o.knownSize = count
	return o.knownSize, nil
}

func (o *DiskPool) Falloc(limit int) error {
	priorSize := o.knownSize
	for i := 0; i < limit; i++ {
		pageId := priorSize + i
		filename := path.Join(o.path, fmt.Sprintf("page_%d", pageId))
		fh, err := os.Open(filename)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				os.WriteFile(filename, []byte{}, 0600)
			} else {
				return err
			}

		}
		fh.Close()

	}
	o.knownSize += limit
	return nil
}
func (o *DiskPool) ReadFrame(idx int) (*PageFrame, error) {
	if idx > o.knownSize {
		return nil, fmt.Errorf("frame index too large: %d", idx)
	}
	filename := path.Join(o.path, fmt.Sprintf("page_%d", idx))
	b, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return NewPageFrame(b), nil
}

func (o *DiskPool) WriteFrame(idx int, pf *PageFrame) error {
	if idx > o.knownSize {
		return fmt.Errorf("frame index too large: %d", idx)
	}
	filename := path.Join(o.path, fmt.Sprintf("page_%d", idx))
	os.WriteFile(filename, pf.frame, 0600)
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

type PageFrame struct {
	// Does the mutex belong here? probably.
	m     sync.RWMutex
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

// DataClone returns a copy of the underlying data. Mutations of the
// returned data will not affect the underlying data.
func (o *PageFrame) DataClone() []byte {
	o.m.RLock()
	defer o.m.RUnlock()
	copy := make([]byte, len(o.frame))
	for i := 0; i < len(o.frame); i++ {
		copy[i] = o.frame[i]
	}
	return copy
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
// pure function with respect to the page frame. DataClone should be
// used if the result is expected to be modified.
func (o *PageFrame) WithRead(f func([]byte)) {
	o.m.RLock()
	defer o.m.RUnlock()
	f(o.frame)
}

// WithWrite - Takes a write lock, sets the Dirty bit, passes the
// internal data in, and the operating function is error-able. Use for
// mutating operations.
func (o *PageFrame) WithWrite(f func([]byte) error) error {
	o.m.Lock()
	defer o.m.Unlock()
	o.dirty = true
	return f(o.frame)
}

type BufferPoolId = int

type FramePoolId = int

type BufferPool struct {
}
