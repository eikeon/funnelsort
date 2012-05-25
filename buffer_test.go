package funnelsort_test

import (
	"testing"

	"github.com/eikeon/funnelsort"
)

func TestBuffer(t *testing.T) {
	funnelsort.NewItem = newItem

	out := funnelsort.NewBufferSize(5)
	out.Write(&intItem{23})
	out.Write(&intItem{34})
	out.Write(&intItem{45})
	if out.Read().(*intItem).value != 23 {
		t.Fail()
	}
	if out.Read().(*intItem).value != 34 {
		t.Fail()
	}
	if out.Read().(*intItem).value != 45 {
		t.Fail()
	}
}

// func TestLargeBuffer(t *testing.T) {
// 	NewBuffer(1 << 33).Close()
// }

// func TestBufferLeak(t *testing.T) {
//  	for i := 1<<20; i>0; i-- {
//  		out := NewBuffer(1<<10)
//  		out.Read()
// 		out.Close()
//  	}
// }
