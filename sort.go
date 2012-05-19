package funnelsort

import (
	"sort"
	//"log"
	"math"
)

type Reader interface {
	Read() Item
}

type Writer interface {
	Write(i Item)
}

const p = uint(14)
const kSquared = 1 << p
var itemArray [kSquared]Item

func manual(in Reader) (items itemSlice, done bool) {
	items = itemSlice(itemArray[0:0])
	for i := 0; i < kSquared; i++ {
		if item := in.Read(); item != nil {
			items = append(items, item)
		} else {
			done = true
			break
		}
	}
	sort.Sort(items)
	return
}

func FunnelSort(in Reader, out Writer) {
	items, done := manual(in)
	if done {
		for _, item := range items {
			out.Write(item)
		}
		return
	}

	var buffers []Buffer
	kMax := 1 << (p / 2)
	top:
	for j := 0; j < kMax; j++ {
		buffer := NewBuffer(1 << 63)
		for _, item := range items {
			buffer.Write(item)
		}
		buffers = append(buffers, buffer)
		if done {
			break
		} else {
			items, done = manual(in)
		}
	}
	
	if done {
		merge(buffers, &OBuffer{out})
	} else {
		buffer := NewBuffer(1 << 63)
		merge(buffers, buffer)
		buffers = buffers[0:0]
		buffers = append(buffers, buffer)
		kMax = int(math.Sqrt(float64(buffer.Unread())))
		goto top
	}
}	


func merge(buffers []Buffer, out Buffer)  {
	f := NewFunnelK(len(buffers))

	// pad the remaining input with empty buffers
	for len(buffers) < int(f.K()) {
		buffer := &itemBuffer{make(itemSlice, 0)}
		buffers = append(buffers, buffer)
	}

	//log.Println(f.K(), len(buffers))

	f.Fill(buffers, out)
	f.Close()
	for i := uint64(0); i < f.K(); i++ { // ?
		buffers[i].Close()
	}
	return
}
