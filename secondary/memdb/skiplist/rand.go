package skiplist

import "math/rand"

const MaxRandBufferSize = 1024

func randGen() chan float32 {
	rnd := rand.New(rand.NewSource(int64(rand.Int())))
	ch := make(chan float32, MaxRandBufferSize)
	go func() {
		for {
			ch <- rnd.Float32()
		}
	}()

	return ch
}
