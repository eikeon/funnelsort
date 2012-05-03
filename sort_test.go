package funnelsort

import (
	"math/rand"
	"testing"
	"encoding/binary"
	"unsafe"
)

const KEY_SIZE = int(unsafe.Sizeof(uint64(0)))
const VALUE_SIZE = 2048
const ITEM_SIZE = KEY_SIZE + VALUE_SIZE

type intItem struct {
	key uint64
	value [VALUE_SIZE]byte
}

func (i *intItem) Write(b []byte) {
	binary.LittleEndian.PutUint64(b[0:SIZE], i.key)
}

func (i *intItem) Read(b []byte) {
	i.key = binary.LittleEndian.Uint64(b[0 : SIZE])
}

func (i *intItem) Less(b Item) bool {
	return i.key < b.(*intItem).key
}

//func lessItem(a, b Item) bool { return a.(*intItem).key < b.(*intItem).key }

func newItem() Item {
	return &intItem{}
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
		return &intItem{key:uint64(rand.Int63())}
	}
	panic("")
}

func TestFunnelSort(t *testing.T) {
	n := uint64(1 << 14)
	t.Log("n:", n)
	increasing := &Increasing{}
	NewItem = newItem
	SetSize(ITEM_SIZE)
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
	SetSize(ITEM_SIZE)
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
