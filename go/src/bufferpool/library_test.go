package bufferpool

import (
	"fmt"
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
		assert.Nil(t, bp.WriteFrame(i, []byte(fmt.Sprintf("X-%d", i))))
	}

	err = bp.FSync()

	assert.Nil(t, err)
}
