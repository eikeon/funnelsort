package funnelsort

import (
	"testing"
)

func TestBuffer(t *testing.T) {
	out := NewBuffer(5)
	out.Write(23)
	out.Write(34)
	out.Write(45)
	if out.Read() != 23 {
		t.Fail()
	}
	if out.Read() != 34 {
		t.Fail()
	}
	if out.Read() != 45 {
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
