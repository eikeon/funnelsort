package funnel

import (
	//"fmt"
	"math"
	"sort"
)

func FunnelSort(in <-chan uint64, N uint64) <-chan uint64 {
	const α = 1 << 1  // α >= 1
	const z = 2       // z-way base merger
	const d = 3       // k-funnels generating output of size k^d
	out := make(chan uint64, 1024)
	if N <= α*z*d {
		buffer := make(ItemSlice, 0, N)
		for item := range in {
			buffer = append(buffer, item)
		}
		sort.Sort(buffer)
		go func() {
			for _, item := range buffer {
				out <- item
			}
			close(out)
		}()
	} else {
		// split in into roughly equal-sized arrays Si, 0 ≤ i < (|in|/α)1/d
		size := uint64(math.Pow(float64(float64(N)/float64(α)), 1./d))
		h := uint64(math.Floor(math.Log2(float64(size))))
		f := NewFunnel(h)
		k := f.K()
		//fmt.Println("size:", size, "k:", k)
		S := make([]Buffer, k)
		bsize := uint64(math.Ceil(float64(N) / float64(k)))
		for i := uint64(0); i < k; i++ {
			var n uint64
			if i*bsize+bsize > N {
				n = N - (i*bsize + bsize)
				if n > bsize {
					panic("boo")
				}
			} else {
				n = bsize
			}
			sin := make(chan uint64, 1024)
			go func(n uint64) {
				for i := uint64(0); i < n; i++ {
					sin <- (<-in)
				}
				close(sin)
			}(n)

			sout := FunnelSort(sin, n)

			// TODO: replace the following with:
			//   S[i] = NewChBuffer(sout)
			buffer := NewBuffer(n)
			for item := range sout {
				buffer.write(item)
			}
			S[i] = buffer
			if S[i].Length() != n {
				panic("boo")
			}
		}

		outB := NewBuffer(N)
		f.Fill(S, outB)
		f.Close()
		for i := uint64(0); i < k; i++ {
			S[i].Close()
		}
		go func() {
			for i := N; i > 0; i-- {
				out <- outB.read()
			}
			close(out)
			outB.Close()
		}()
	}
	return out
}
