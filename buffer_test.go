package funnelsort

import (
	"testing"
)

func TestBuffer(t *testing.T) {
	NewItem = newItem

	out := NewBuffer(5)
	out.Write(&intItem{23})
	out.Write(&intItem{34})
	out.Write(&intItem{45})
	if uint64(out.Read().(*intItem).Value) != 23 {
		t.Fail()
	}
	if uint64(out.Read().(*intItem).Value) != 34 {
		t.Fail()
	}
	if uint64(out.Read().(*intItem).Value) != 45 {
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
