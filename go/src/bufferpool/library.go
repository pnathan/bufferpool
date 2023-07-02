package bufferpool

import (
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"math/rand"
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

// NewMockPool creates a new MockPool with a given size, or nil if unable to allocate (very surprising), such sus.
func NewMockPool(size int) *MockPool {
	mp := &MockPool{
		frames: map[int]*PageFrame{},
		size:   0,
	}
	if size > 0 {
		err := mp.Falloc(size)
		if err != nil {
			return nil
		}
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
		return nil, fmt.Errorf("%d not in framePool", idx)
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
	loadedFrames   map[int]*PageFrame
	knownPageCount int
	path           string
	m              sync.RWMutex
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
		loadedFrames:   map[int]*PageFrame{},
		knownPageCount: 0,
		path:           directoryPath,
	}
	err = dp.Falloc(limit)
	if err != nil {
		return nil, err
	}
	return dp, nil
}

func (o *DiskPool) Size() int {
	return o.knownPageCount
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
	o.knownPageCount = count
	return o.knownPageCount, nil
}

// Name of page the Disk Pool Uses
func (o *DiskPool) PageFileName(idx int) string {
	return path.Join(o.path, fmt.Sprintf("page_%d", idx))
}

// Falloc creates `limit` more frames in the DiskPool directory.
func (o *DiskPool) Falloc(limit int) error {
	priorSize := o.knownPageCount
	for i := 0; i < limit; i++ {
		pageId := priorSize + i
		filename := o.PageFileName(pageId)
		fh, err := os.Open(filename)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				err := os.WriteFile(filename, []byte{}, 0600)
				if err != nil {
					return err
				}
			} else {
				return err
			}

		}
		_ = fh.Close()
	}
	o.knownPageCount += limit
	return nil
}
func (o *DiskPool) ReadFrame(idx int) (*PageFrame, error) {
	if idx > o.knownPageCount {
		return nil, fmt.Errorf("frame index too large: %d", idx)
	}
	filename := o.PageFileName(idx)
	b, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return NewPageFrame(b), nil
}

func (o *DiskPool) WriteFrame(idx int, pf *PageFrame) error {
	if idx > o.knownPageCount {
		return fmt.Errorf("frame index too large: %d", idx)
	}
	o.m.Lock()
	defer o.m.Unlock()
	filename := o.PageFileName(idx)
	err := os.WriteFile(filename, pf.frame, 0600)
	if err != nil {
		return err
	}
	return nil
}

type PageFrame struct {
	// The relevant data.
	frame []byte
	// Pins denotes how many threads the page is being used by.
	pins int
	// Dirty denotes if the frame has been modified.
	dirty bool
	// Whenever anything in the struct is read, the mutex is set to RLock.
	// Whenever anything in the struct is written, the mutex is set to Lock.
	m sync.RWMutex
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
func (pf *PageFrame) DataClone() []byte {
	pf.m.RLock()
	defer pf.m.RUnlock()
	bytes := make([]byte, len(pf.frame))
	for i := 0; i < len(pf.frame); i++ {
		bytes[i] = pf.frame[i]
	}
	return bytes
}

// IncPin when the data is being used by a thread.
func (pf *PageFrame) IncPin() {
	pf.m.Lock()
	defer pf.m.Unlock()
	pf.pins += 1
}

// DecPin when the data is no longer being used by a thread.
func (pf *PageFrame) DecPin() {
	pf.m.Lock()
	defer pf.m.Unlock()
	pf.pins -= 1
}

// Pins returns the number of threads using the data.
func (pf *PageFrame) Pins() int {
	pf.m.RLock()
	defer pf.m.RUnlock()
	return pf.pins
}

// IsDirty returns if the data has been modified. No locking performed; use Take/ReleaseLock.
func (pf *PageFrame) IsDirty() bool {
	return pf.dirty
}

// TakeLock takes the lock for the pageframe
func (pf *PageFrame) TakeLock() {
	pf.m.Lock()
}

// ReleaseLock releases the lock for the pageframe
func (pf *PageFrame) ReleaseLock() {
	pf.m.Unlock()
}

// WithRead - Pass the data into the passed-in function.  The function
// should not be able to return errors, etc. That is, it should be a
// pure function with respect to the page frame. DataClone should be
// used if the result is expected to be modified.
func (pf *PageFrame) WithRead(f func([]byte)) {
	pf.m.RLock()
	defer pf.m.RUnlock()
	f(pf.frame)
}

// WithWrite - Takes a write lock, sets the Dirty bit, passes the
// internal data in, and the operating function is error-able. Use for
// mutating operations.
func (pf *PageFrame) WithWrite(f func(*[]byte) error) error {
	pf.m.Lock()
	defer pf.m.Unlock()
	pf.dirty = true
	return f(&pf.frame)
}

type Evictor interface {
	// Evict selects a victim BufferPoolId candidate and returns it.
	// Evict does not delete from the lru; Evict is stateless.
	Evict(pages []*PageFrame, pageFrameIndex map[FramePoolId]BufferPoolId, lru *UniqueStack[BufferPoolId]) (FramePoolId, error)
}

type RandomEvictor struct{}

func (o RandomEvictor) Evict(pages []*PageFrame, _ map[FramePoolId]BufferPoolId, _ *UniqueStack[BufferPoolId]) (FramePoolId, error) {
	potential := rand.Int() % (len(pages) - 1)
	for true {
		if pages[potential].Pins() > 0 {
			potential = rand.Int() % (len(pages) - 1)
		} else {
			break
		}
	}
	return potential, nil
}

type BottomEvictor struct{}

func (o BottomEvictor) Evict(_ []*PageFrame, frame2Buf map[FramePoolId]BufferPoolId, lru *UniqueStack[BufferPoolId]) (FramePoolId, error) {
	pageId := lru.Bottom()
	for k, v := range frame2Buf {
		if v == pageId {
			return k, nil
		}
	}
	return -1, fmt.Errorf("state incoherence error in eviction: unable to find selected lru pageId (%v) in pages", pageId)
}

// BufferPoolId  indexes into the buffer framePool, which is a small framePool.
type BufferPoolId = int

// FramePoolId can be considered to be the oid of the frame.
type FramePoolId = int

type BufferPool struct {
	// Size of BufferPool. Constant for framePool duration.
	// This multiplexes onto the Framepool.
	size int
	// List of pages. Indexed by BufferPoolId.
	// len(pages) == size
	pages []*PageFrame
	// buf2Frame maps the page ids to framepool ids.
	buf2Frame map[BufferPoolId]FramePoolId
	// frame2Buf maps the frame framePool ids [0-size) to
	// BufferPoolIds. Not all framepool ids are valid
	// BufferPoolIds - the range is sparse
	frame2Buf map[FramePoolId]BufferPoolId
	// LRU of buffer indices to framepools
	lru *UniqueStack[BufferPoolId]
	// FramePool that handles permanence
	framePool FramePool
	// evictor, a pluggable system.
	evictor Evictor

	// These two will be set on failures in `defers`
	failureDetected bool
	failure         error
}

func NewBufferPool(size int, pool FramePool, evictor Evictor) *BufferPool {
	pages := make([]*PageFrame, size)
	// Probably not required but leaving as is.
	for i := 0; i < size; i++ {
		pages[i] = nil
	}
	return &BufferPool{
		size:      size,
		pages:     pages,
		buf2Frame: map[BufferPoolId]FramePoolId{},
		frame2Buf: map[FramePoolId]BufferPoolId{},
		lru:       NewUniqueStack[int](),
		framePool: pool,
		evictor:   evictor,
	}
}

// A Slab is a pseudocontiguous chunk of memory overlaying a bufferpool
type Slab struct {
	// The bufferpool that this slab is a part of
	pool *BufferPool
	// Bytes per stride, for each slot in the bufferpool
	strideWidth int
	// The size of the slab
	size int
}

func NewSlab(frameSize int, backingPath string) (*Slab, error) {
	fp, err := NewDiskPool(frameSize, backingPath)
	if err != nil {
		return nil, err
	}
	return &Slab{
		size:        0,
		strideWidth: 16,
		pool:        NewBufferPool(100, fp, BottomEvictor{}),
	}, nil
}

// Put - Writes bytes to index in the slab
func (slab *Slab) Put(startingIndex int, data []byte) error {
	return fmt.Errorf("unable to put to a slab thank you")
	/*
		// find the starting buffer to write to
		targetBufferStart := startingIndex / slab.strideWidth
		targetIndexModulus := startingIndex % slab.strideWidth
		width := len(data)/slab.strideWidth + 1

		// We acquire a page, write from the starting index to the end of the page, then release the page.
		// We then acquire the next page, write from the start of the page to the end of the data, then release the page.
		// We repeat this process until we have written all of the data.
		data_counter := 0
		for bufferIndex := targetBufferStart; bufferIndex < targetBufferStart+width; bufferIndex++ {
			if err := func() error {
				page, err := slab.pool.AcquirePage(bufferIndex)
				defer func() { _ = slab.pool.ReleasePage(bufferIndex) }()

				if err != nil {
					return err
				}
				page.WithWrite(func(d *[]byte) error {
					for i := bufferIndex * slab.strideWidth; i < (bufferIndex+1)*slab.strideWidth && data_counter < len(data); i++ {
						(*d)[i] = data[data_counter]
						data_counter++
					}
					return nil
				})
				return nil
			}(); err != nil {
				return err
			}
		}

		for i := target; i < target+width; i++ {
			bufferIndex := i % slab.strideWidth
			page, err := slab.pool.AcquirePage(bufferIndex)
			if err != nil {
				return err
			}
			page.WithWrite(func(d *[]byte) error {
				*d = data
			})
			defer func() { _ = slab.pool.ReleasePage(target) }()
		}
	*/
}

// Get - Gets bytes from index in the slab.
func (slab *Slab) Get(idx int) ([]byte, error) {
	return nil, fmt.Errorf("unable to GET from a slab thank you")
	/*
		if idx > bp.framePool.Size() || idx < 0 {
			return nil, fmt.Errorf("index out of range: %d", idx)
		}
		pf, err := bp.AcquirePage(idx)
		if err != nil {
			return nil, err
		}
		defer func() { _ = bp.ReleasePage(idx) }()
		return pf.DataClone(), nil
	*/
}

// ReleasePage decrements the pin of `idx`.
func (bp *BufferPool) ReleasePage(idx FramePoolId) error {
	if idx > bp.size || idx < 0 {
		return fmt.Errorf("index out of range: %d", idx)
	}
	pageIdx, ok := bp.frame2Buf[idx]
	if !ok {
		return fmt.Errorf("not valid page: %d", idx)
	}
	bp.pages[pageIdx].DecPin()
	return nil
}

// AcquirePage - page is acquired from its data source, if need be, then the Pin is incremented.
func (bp *BufferPool) AcquirePage(idx FramePoolId) (*PageFrame, error) {
	p, err := bp.GetPage(idx)
	if err != nil {
		return nil, err
	}
	p.IncPin()
	return p, nil
}

func (bp *BufferPool) GetPage(idx FramePoolId) (*PageFrame, error) {
	if idx > bp.framePool.Size() - -1 {
		return nil, fmt.Errorf("bufferpool index out of range %d", idx)
	}

	_, ok := bp.buf2Frame[idx]
	// if we don't have the page loaded...
	if !ok {
		// if we are full...
		if len(bp.buf2Frame) == bp.size {

			// TYPE ERRORS
			victimIndex, err := bp.evictor.Evict(bp.pages,
				bp.frame2Buf, bp.lru)
			if err != nil {
				return nil, err
			}
			victimPageId := bp.frame2Buf[victimIndex]
			if bp.pages[victimIndex].IsDirty() {
				err := bp.framePool.WriteFrame(victimPageId, bp.pages[victimIndex])
				if err != nil {
					return nil, err
				}
			}
			bp.pages[victimIndex] = nil
			delete(bp.buf2Frame, victimPageId)
			delete(bp.frame2Buf, victimIndex)
			err = bp.lru.Delete(victimPageId)
			if err != nil {
				return nil, err
			}

			// postcondition: we have one empty slot
		}
		var target_index *int
		for i := 0; i < bp.size; i++ {
			if bp.pages[i] == nil {
				target_index = &i
				break
			}
		}
		if target_index == nil {
			return nil, fmt.Errorf("unable to find empty slot, error, error")
		}
		frame, err := bp.framePool.ReadFrame(idx)
		if err != nil {
			return nil, err
		}
		bp.pages[*target_index] = frame
		bp.buf2Frame[*target_index] = idx
		bp.frame2Buf[idx] = *target_index
		//postcondition: the idx loaded and in the slot
	}
	bp.lru.Push(idx)
	return bp.pages[bp.frame2Buf[idx]], nil
}

func (bp *BufferPool) WritePage(idx FramePoolId, data []byte) error {
	page, err := bp.AcquirePage(idx)
	if err != nil {
		return err
	}
	defer func() {
		err := bp.ReleasePage(idx)
		if err != nil {
			bp.failureDetected = true
			bp.failure = err
			return
		}
	}()
	err = page.WithWrite(func(b *[]byte) error {
		*b = data
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (bp *BufferPool) FSync() error {
	for _, fpId := range bp.buf2Frame {
		page := bp.pages[fpId]
		e := FSyncSlot(page, bp, fpId)
		if e != nil {
			return e
		}
	}
	return nil
}

func FSyncSlot(page *PageFrame, bp *BufferPool, fpId FramePoolId) error {
	page.TakeLock()
	defer page.ReleaseLock()
	if page.IsDirty() {
		err := bp.framePool.WriteFrame(fpId, page)
		if err != nil {
			return err
		}
	}
	return nil
}
