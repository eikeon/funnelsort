package funnel

import (
	"testing"
)

func TestBuffer(t *testing.T) {
	out := NewBuffer(5)
	out.write(23)
	out.write(34)
	out.write(45)
	if out.read() != 23 {
		t.Fail()
	}
	if out.read() != 34 {
		t.Fail()
	}
	if out.read() != 45 {
		t.Fail()
	}
}

// func TestLargeBuffer(t *testing.T) {
// 	NewBuffer(1 << 33).Close()
// }

// func TestBufferLeak(t *testing.T) {
//  	for i := 1<<20; i>0; i-- {
//  		out := NewBuffer(1<<10)
//  		out.read()
// 		out.Close()
//  	}
// }
