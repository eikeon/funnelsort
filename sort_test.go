package funnel

import (
	"math/rand"
	"testing"
)

type Increasing struct {
	outOfOrder bool
	last       uint64
}

func (w *Increasing) write(item uint64) {
	if w.last > item {
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

func (r *Random) read() uint64 {
	if r.unread > 0 {
		r.unread -= 1
		return uint64(rand.Int63())
	}
	panic("")
}

func TestFunnelSort(t *testing.T) {
	n := uint64(1 << 14)
	t.Log("n:", n)
	increasing := &Increasing{}
	FunnelSort(&Random{n}, increasing)

	if increasing.outOfOrder {
		t.Error("output not sorted")
		t.FailNow()
	}
}

func Sort(p uint) bool {
	n := uint64(1 << p)
	increasing := &Increasing{}
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
