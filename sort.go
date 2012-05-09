package funnelsort

import (
	"math"
	"sort"
)

type Reader interface {
	Read() Item
	Unread() uint64 // TODO: big.Int ?
}

type Writer interface {
	Write(i Item)
	// Written() uint64
}

type limitReader struct {
	reader Reader
	unread uint64
}

func (r *limitReader) Unread() uint64 {
	return r.unread
}

func (r *limitReader) Read() Item {
	if r.unread > 0 {
		r.unread -= 1
		item := r.reader.Read()
		return item
	}
	panic("")
}

func FunnelSort(in Reader, out Writer) {
	const α = 1 << 15 // α >= 1
	const z = 2       // z-way base merger
	const d = 3       // k-funnels generating output of size k^d
	N := in.Unread()
	if N <= α*z*d {
		buffer := make(itemSlice, 0, N)
		for i := N; i > 0; i-- {
			buffer = append(buffer, in.Read())
		}
		sort.Sort(buffer)
		for _, item := range buffer {
			out.Write(item)
		}
	} else {
		// split in into roughly equal-sized arrays Si, 0 ≤ i < (|in|/α)1/d
		size := uint64(math.Pow(float64(float64(N)/float64(α)), 1./d))
		h := uint64(math.Floor(math.Log2(float64(size))))
		f := NewFunnel(h)
		k := f.K()
		S := make([]Buffer, k)
		bsize := uint64(math.Ceil(float64(N) / float64(k)))
		remaining := N
		for i := uint64(0); i < k; i++ {
			var n uint64
			if i == k-1 {
				n = remaining
			} else {
				n = bsize
				remaining -= n
			}
			S[i] = NewBuffer(n)
			FunnelSort(&limitReader{in, n}, S[i])
		}
		b := NewBuffer(N)
		f.Fill(S, b)
		for i := uint64(0); i < k; i++ {
			S[i].Close()
		}
		for i := N; i > 0; i-- {
			out.Write(b.Read())
		}
		b.Close()
		f.Close()
	}
}
