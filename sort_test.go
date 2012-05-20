package funnelsort

import (
	"encoding/binary"
	"math/rand"
	"testing"
)

type intItem struct {
	value uint64
}

func (i *intItem) Less(b Item) bool {
	return i.value < b.(*intItem).value
}

func (i *intItem) Key() int64 {
	return int64(i.value)
}

func (i *intItem) Value() []byte {
	v := make([]byte, 8)
	binary.PutUvarint(v, i.value)
	return v
}

// func lessItem(a, b Item) bool {
// 	return a.(*intItem).Value < b.(*intItem).Value
// }

func newItem(key int64, value []byte) Item {
	return &intItem{value: uint64(key)}
}

type Increasing struct {
	outOfOrder bool
	last       Item
}

func (w *Increasing) Write(item Item) {
	if w.last != nil && item.Less(w.last) {
		w.outOfOrder = true
	}
	w.last = item
}

type Random struct {
	unread uint64
}

func (r *Random) Unread() uint64 {
	return r.unread
}

func (r *Random) Read() Item {
	if r.unread > 0 {
		r.unread -= 1
		return &intItem{uint64(rand.Int63())}
	}
	panic("")
}

func TestFunnelSort(t *testing.T) {
	n := uint64(1 << 14)
	increasing := &Increasing{}
	NewItem = newItem
	FunnelSort(&Random{n}, increasing)

	if increasing.outOfOrder {
		t.Error("output not sorted")
		t.FailNow()
	}
}

func Sort(p uint) bool {
	n := uint64(1 << p)
	increasing := &Increasing{}
	NewItem = newItem
	FunnelSort(&Random{n}, increasing)
	return increasing.outOfOrder
}

func Benchmark4(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if Sort(4) {
			b.FailNow()
		}
	}
}

func Benchmark5(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if Sort(5) {
			b.FailNow()
		}
	}
}

func Benchmark6(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if Sort(6) {
			b.FailNow()
		}
	}
}
