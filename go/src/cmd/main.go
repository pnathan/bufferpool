package main

import (
	"fmt"

	"gitlab.com/pnathan/bufferpool/src/bufferpool"
)

func main() {
	var fp bufferpool.FramePool
	fp = bufferpool.NewMockPool(8)
	fp.WriteFrame(0, bufferpool.NewPageFrame([]byte("foo")))
	b, err := fp.ReadFrame(0)
	if err != nil {
		panic("omg")
	}
	b.WithRead(
		func(data []byte) {
			fmt.Println(string(data))
		})
}
