package funnel

import (
	"math/rand"
)

// TODO: type Item
type ItemSlice []uint64

func (p ItemSlice) Len() int           { return len(p) }
func (p ItemSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p ItemSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func RandomItems(n uint64) <-chan uint64 {
	c := make(chan uint64, 1024)
	go func() {
		for i := uint64(n); i > 0; i-- {
			c <- uint64(rand.Int())
		}
		close(c)
	}()
	return c
}

func Increasing(output <-chan uint64, n uint64) bool {
	i := uint64(0)
	var last uint64
	for item := range output {
		if i > 0 {
			if last > item {
				return false
			}
		}
		last = item
		i += 1
	}
	if i != n {
		return false
	}
	return true
}
