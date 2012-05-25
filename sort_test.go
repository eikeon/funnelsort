package funnelsort_test

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/eikeon/funnelsort"
)

type intItem struct {
	value uint64
}

func (i *intItem) Less(b funnelsort.Item) bool {
	return i.value < b.(*intItem).value
}

func (i *intItem) Bytes() []byte {
	b := make([]byte, 8)
	b[0] = byte(i.value)
	b[1] = byte(i.value >> 8)
	b[2] = byte(i.value >> 16)
	b[3] = byte(i.value >> 24)
	b[4] = byte(i.value >> 32)
	b[5] = byte(i.value >> 40)
	b[6] = byte(i.value >> 48)
	b[7] = byte(i.value >> 56)
	return b
}

func newItem(b []byte) funnelsort.Item {
	return &intItem{binary.LittleEndian.Uint64(b[0:8])}
}

type Increasing struct {
	outOfOrder bool
	last       funnelsort.Item
}

func (w *Increasing) Write(item funnelsort.Item) {
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

func (r *Random) Read() funnelsort.Item {
	if r.unread > 0 {
		r.unread -= 1
		return &intItem{uint64(rand.Int63())}
	}
	return nil
}

func TestFunnelSort(t *testing.T) {
	n := uint64(1 << 14)
	increasing := &Increasing{}
	funnelsort.NewItem = newItem
	funnelsort.FunnelSort(&Random{n}, increasing)

	if increasing.outOfOrder {
		t.Error("output not sorted")
		t.FailNow()
	}
}

func Sort(p uint) bool {
	n := uint64(1 << p)
	increasing := &Increasing{}
	funnelsort.NewItem = newItem
	funnelsort.FunnelSort(&Random{n}, increasing)
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
