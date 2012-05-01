package funnel

import (
	"math"
	"sort"
)

type Reader interface {
	read() uint64
	Unread() uint64 // TODO: big.Int ?
}

type Writer interface {
	write(i uint64)
	// Written() uint64
}

type limitReader struct {
	reader Reader
	unread uint64
}

func (r *limitReader) Unread() uint64 {
	return r.unread
}

func (r *limitReader) read() uint64 {
	if r.unread > 0 {
		r.unread -= 1
		item := r.reader.read()
		return item
	}
	panic("")
}

//func FunnelSort(in <-chan uint64, N uint64) <-chan uint64 {
func FunnelSort(in Reader, out Writer) {
	const α = 1 << 5 // α >= 1
	const z = 2      // z-way base merger
	const d = 3      // k-funnels generating output of size k^d
	N := in.Unread()
	if N <= α*z*d {
		buffer := make(ItemSlice, 0, N)
		for i := N; i > 0; i-- {
			buffer = append(buffer, in.read())
		}
		sort.Sort(buffer)
		for _, item := range buffer {
			out.write(item)
		}
	} else {
		// split in into roughly equal-sized arrays Si, 0 ≤ i < (|in|/α)1/d
		size := uint64(math.Pow(float64(float64(N)/float64(α)), 1./d))
		h := uint64(math.Floor(math.Log2(float64(size))))
		f := NewFunnel(h)
		k := f.K()
		S := make([]Buffer, k)
		bsize := uint64(math.Ceil(float64(N) / float64(k)))
		for i := uint64(0); i < k; i++ {
			var n uint64
			if i*bsize+bsize > N {
				n = N - (i*bsize + bsize)
			} else {
				n = bsize
			}
			S[i] = NewChBuffer(n)
			FunnelSort(&limitReader{in, n}, S[i])
		}
		b := NewChBuffer(N)
		f.Fill(S, b)
		f.Close()
		for i := N; i > 0; i-- {
			out.write(b.read())
		}
	}
}
