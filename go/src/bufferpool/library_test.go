package bufferpool

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"

	assert "github.com/stretchr/testify/assert"
)

func TestHappyMockPool(t *testing.T) {
	x := NewMockPool(0)
	assert.Equal(t, x.Size(), 0)
	_, err := x.ReadFrame(0)
	assert.Error(t, err)
	err = x.WriteFrame(0, NewPageFrame([]byte("abc")))
	if err != nil {
		return
	}
	f, err := x.ReadFrame(0)
	assert.Nil(t, err)
	assert.Equal(t, string(f.frame), "abc")

	err = x.Falloc(3)
	if err != nil {
		return
	}
	assert.Equal(t, x.Size(), 3)
	value, err := x.AssessSize()
	assert.Nil(t, err)
	assert.Equal(t, value, 3)

	err = x.Falloc(4)
	if err != nil {
		return
	}
	assert.Equal(t, x.Size(), 7)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytesRmndr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

func TestMultiThreadedMockPool(t *testing.T) {
	x := NewMockPool(3)
	var wg sync.WaitGroup
	datalist := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		datalist[i] = RandStringBytesRmndr(rand.Int() % 256)
	}
	for i, s := range datalist {
		wg.Add(1)
		go func(idx int, data string) {
			defer wg.Done()
			x.WriteFrame(idx, NewPageFrame([]byte(data)))
		}(i, s)
	}
	wg.Wait()
	for i, s := range datalist {
		wg.Add(1)
		go func(idx int, data string) {
			defer wg.Done()
			f, err := x.ReadFrame(idx)
			assert.Nil(t, err)
			assert.Equal(t, string(f.frame), data)

		}(i, s)
	}
	wg.Wait()

}

func TestDiskPoolSizing(t *testing.T) {
	td := t.TempDir()
	dp, err := NewDiskPool(4, td)
	assert.Nil(t, err)
	assert.Equal(t, 4, dp.Size())
	sz, err := dp.AssessSize()
	assert.Nil(t, err)
	assert.Equal(t, 4, sz)

	dp2, err := NewDiskPool(0, td)

	assert.Equal(t, 0, dp2.Size())

	sz2, err := dp2.AssessSize()
	assert.Nil(t, err)
	assert.Equal(t, 4, sz2)
	assert.Equal(t, 4, dp2.Size())

}

func TestDiskPoolRWSimple(t *testing.T) {
	td := t.TempDir()
	dp, err := NewDiskPool(4, td)
	assert.Nil(t, err)
	dp.WriteFrame(0, NewPageFrame([]byte("abc")))
	f, err := dp.ReadFrame(0)
	assert.Nil(t, err)
	assert.Equal(t, string(f.DataClone()), "abc")
	assert.Equal(t, string(f.frame), "abc")

}

func TestBasicBufferPool(t *testing.T) {
	bp := NewBufferPool(10, NewMockPool(10), RandomEvictor{})
	assert.NotNil(t, bp)
}

func TestDiskPool_PageFileName(t *testing.T) {
	tests := []struct {
		name      string
		fields    *DiskPool
		wantedIdx int
		want      string
	}{
		// Test cases to generate names of the pagefile based on wanted fields, known page count and path
		{
			name: "Test 1",
			fields: &DiskPool{
				loadedFrames:   make(map[int]*PageFrame),
				knownPageCount: 0,
				path:           "C:\\Users",
			},
			wantedIdx: 10,
			want:      "C:\\Users/page_10",
		},
		{
			name: "Test 2",
			fields: &DiskPool{
				loadedFrames:   make(map[int]*PageFrame),
				knownPageCount: 2,
				path:           "C:\\Users",
			},
			wantedIdx: 1,
			want:      "C:\\Users/page_1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &DiskPool{
				loadedFrames:   tt.fields.loadedFrames,
				knownPageCount: tt.fields.knownPageCount,
				path:           tt.fields.path,
			}
			assert.Equalf(t, tt.want, o.PageFileName(tt.wantedIdx), "PageFileName(%v)", tt.wantedIdx)
		})
	}
}

// This is a test for the FSync method of the BufferPool, using the MockPool
func TestBufferPool_FSync(t *testing.T) {
	td := os.TempDir()
	t.Logf("temp dir %s", td)
	mp, err := NewDiskPool(10, td)
	if err != nil {
		return
	}
	bp := NewBufferPool(3, mp, RandomEvictor{})
	assert.NotNil(t, bp)
	// Here we write to the buffer pool in each of the 3 frames
	for i := 0; i < 3; i++ {
		assert.Nil(t, bp.WritePage(i, []byte(fmt.Sprintf("X-%d", i))))
	}

	err = bp.FSync()
	for i := 0; i < 3; i++ {
		b, err := ioutil.ReadFile(fmt.Sprintf("%s/page_%d", td, i))
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, fmt.Sprintf("X-%d", i), string(b))
	}

	assert.Nil(t, err)
}

// this is a test for the Evict method of the BottomEvictor, which should always take the bottom of the UniqueStack[BufferPoolId]
func TestBottomEvictor_Evict(t *testing.T) {
	// create a new BottomEvictor
	b := BottomEvictor{}
	// create a new UniqueStack
	us := NewUniqueStack[BufferPoolId]()
	pfi := map[FramePoolId]BufferPoolId{}
	// add 3 BufferPoolId's to the UniqueStack
	for i := 0; i < 3; i++ {
		us.Push(BufferPoolId(i))
		pfi[100+i] = i
	}
	// create empty page frame list
	pfl := make([]*PageFrame, 0)

	// call the Evict method on the BottomEvictor
	// Note that Evict returns a victim, it does not remove the victim
	victim, err := b.Evict(pfl, pfi, us)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, pfi[victim], us.Bottom())
	// Check if it *should* be the bottom.
	assert.Equal(t, FramePoolId(100), victim)
	assert.Nil(t, us.Delete(pfi[victim]))

	data := us.OrderedRead()
	// data is equal to 1,2
	assert.Equal(t, []BufferPoolId{1, 2}, data)

	// Add another to the Top
	us.Push(BufferPoolId(3))
	pfi[1003] = 3

	// add a previously used buffer
	us.Push(BufferPoolId(1))
	// Assert: we should have 2 as the victim.
	// 1 is the top, followed by 3, followed by 2
	assert.Equal(t, BufferPoolId(2), us.Bottom())
	t.Logf("us: %v", us.OrderedRead())
	t.Logf("pfi: %v", pfi)
	victim, err = b.Evict(pfl, pfi, us)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, us.Bottom(), pfi[victim])
}

func TestEndGeneralUsageBufferPool(t *testing.T) {
	d := t.TempDir()
	bp, err := NewSlab(4, d)
	if err != nil {
		t.Fatal(err)
	}

}
