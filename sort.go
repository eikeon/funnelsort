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

func FunnelSort(in Reader, out Writer) {
	const p = uint(12)
	const kSquared = 1 << p
	kMax := 1 << (p / 2)

	var done bool
	var buffers []Buffer
	top:
	for j := 0; j < kMax; j++ {
		buffer := &itemBuffer{make(itemSlice, 0, kSquared)}
		for i := 0; i < kSquared; i++ {
			item := in.Read()
			if item != nil {
				buffer.buf = append(buffer.buf, item)
			} else {
				done = true
				break
			}
		}
		sort.Sort(buffer.buf)
		if len(buffer.buf)>0 {
			buffers = append(buffers, buffer)
		}
		if done == true {
			break
		}
	}
	
	if done && len(buffers)==1 {
		buffer := buffers[0]
		for {
			item := buffer.Read()
			if item != nil {
				out.Write(item)
			} else {
				break
			}
		}
	} else {
		k := uint64(hyperceil(float64(len(buffers))))
		h := uint64(math.Floor(math.Log2(float64(k))))
		f := NewFunnel(h)
		fout := NewBuffer(1 << 63)

		// pad the remaining input with empty buffers
		for len(buffers) < int(k) {
			buffer := &itemBuffer{make(itemSlice, 0)}
			buffers = append(buffers, buffer)
		}

		f.Fill(buffers, fout)

		for i := uint64(0); i < k; i++ {
			buffers[i].Close()
		}
		if done {
			for i := fout.Unread(); i > 0; i-- {
				out.Write(fout.Read())
			}
			fout.Close()
			f.Close()
		} else {
			kMax = int(math.Sqrt(float64(fout.Unread())))
			buffers = buffers[0:0]
			buffers = append(buffers, fout)
			goto top
		}
	}
}
