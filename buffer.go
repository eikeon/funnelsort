package funnelsort

import (
	"encoding/gob"
)

type Buffer interface {
	Reader
	Writer
	peek() Item
	empty() bool
	full() bool
	reset()
	Close()
}

func NewBuffer(n uint64) *FBuffer {
	b := &FBuffer{max: n}
	b.buffer = NewLargeBuffer()
	b.reset()
	return b
}

type FBuffer struct {
	max, unread uint64
	buffer      *LargeBuffer
	enc         *gob.Encoder
	dec         *gob.Decoder
	peekItem    Item
}

func (b *FBuffer) Close() {
	b.buffer.Close()
}

func (b *FBuffer) Unread() uint64 {
	return b.unread
}

func (b *FBuffer) empty() bool {
	return b.unread == 0
}

func (b *FBuffer) full() bool {
	return b.unread == b.max
}

func (b *FBuffer) reset() {
	b.unread = 0
	b.peekItem = nil
	b.enc = gob.NewEncoder(b.buffer)
	b.dec = gob.NewDecoder(b.buffer)
}

func (b *FBuffer) Write(a Item) {
	b.unread += 1
	if err := b.enc.Encode(a); err != nil {
		panic(err)
	}
}

func (b *FBuffer) read() Item {
	item := NewItem()
	if err := b.dec.Decode(item); err != nil {
		panic(err)
	}
	return item
}

func (b *FBuffer) peek() Item {
	if b.peekItem == nil {
		b.peekItem = b.read()
	}
	return b.peekItem
}

func (b *FBuffer) Read() Item {
	item := b.peek()
	b.peekItem = nil
	b.unread -= 1
	return item
}
