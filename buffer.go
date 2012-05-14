package funnelsort

import (
	"bufio"
	"encoding/binary"
	"io"
)

type tweetBuf [4096]byte

var tb tweetBuf

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
	r := bufio.NewReaderSize(b.buffer, 1 << 18)
	w := bufio.NewWriterSize(b.buffer, 1 << 18)
	b.bio = bufio.NewReadWriter(r, w)
	b.reset()
	return b
}

type FBuffer struct {
	max, unread uint64
	buffer      *LargeBuffer
	bio         *bufio.ReadWriter
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
}

func (b *FBuffer) Write(a Item) {
	b.unread += 1

	v := a.Key()

	b.bio.WriteByte(byte(v))
	b.bio.WriteByte(byte(v >> 8))
	b.bio.WriteByte(byte(v >> 16))
	b.bio.WriteByte(byte(v >> 24))
	b.bio.WriteByte(byte(v >> 32))
	b.bio.WriteByte(byte(v >> 40))
	b.bio.WriteByte(byte(v >> 48))
	b.bio.WriteByte(byte(v >> 56))

	l := len(a.Value())

	b.bio.WriteByte(byte(l))
	b.bio.WriteByte(byte(l >> 8))
	b.bio.WriteByte(byte(l >> 16))
	b.bio.WriteByte(byte(l >> 24))

	b.bio.Write(a.Value())
	//b.bio.Flush()
}

func (b *FBuffer) read() Item {
	b.bio.Flush()

	n, err := io.ReadFull(b.bio, tb[0:8])
	if n!=8 || err != nil {
		panic(err)
	}
	key := binary.LittleEndian.Uint64(tb[0:8])

	n, err = io.ReadFull(b.bio, tb[0:4])
	if n!=4 || err != nil {
		panic(err)
	}
        l := binary.LittleEndian.Uint32(tb[0:4])

	n, err = io.ReadFull(b.bio, tb[0:l])
	if n!=int(l) || err != nil {
		panic(err)
	}

	value := tb[0:l]

	bb := make([]byte, l)
	copy(bb, value)

	item := NewItem(int64(key), bb)
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
