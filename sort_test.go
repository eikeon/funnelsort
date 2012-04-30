package funnel

import (
	"testing"
)

func TestRandomItems(t *testing.T) {

	n := uint64(2048)
	c := RandomItems(n)
	i := uint64(0)
	for _ = range c {
		i += 1
		//t.Logf("%d\n", item)
	}
	if i != n {
		t.Error("channel should be empty")
		t.FailNow()
	}
}

func TestFunnelSort(t *testing.T) {
	n := uint64(1 << 15)
	t.Log("n:", n)
	out := FunnelSort(RandomItems(n), n)

	if Increasing(out, n) == false {
		t.Error("output not sorted")
		t.FailNow()
	}
}

func Sort(p uint) bool {
	n := uint64(1 << p)
	return Increasing(FunnelSort(RandomItems(n), n), n)
}

func Benchmark4(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if Sort(4) == false {
			b.FailNow()
		}
	}
}

func Benchmark5(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if Sort(5) == false {
			b.FailNow()
		}
	}
}

func Benchmark6(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if Sort(6) == false {
			b.FailNow()
		}
	}
}
