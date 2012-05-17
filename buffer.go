package funnelsort

import (
	"bufio"
	//"compress/gzip"
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
	b.reset()
	return b
}

type FBuffer struct {
	max, unread uint64
	buffer      *LargeBuffer
	r           *bufio.Reader
	w           *bufio.Writer
	//gzw         *gzip.Writer
	//peekItem    Item
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
	//b.peekItem = nil
	b.buffer.Reset()
	// w, err := gzip.NewWriterLevel(b.buffer, gzip.BestSpeed)
	// if err != nil {
	// 	panic(err)
	// }
	//b.gzw = w
	//b.w = bufio.NewWriterSize(b.gzw, 1 << 18)
	//b.w = bufio.NewWriterSize(b.buffer, 1 << 18)
	b.w = bufio.NewWriterSize(b.buffer, 512)
	b.r = nil
}

func (b *FBuffer) Write(a Item) {
	b.unread += 1

	v := a.Key()

	b.w.WriteByte(byte(v))
	b.w.WriteByte(byte(v >> 8))
	b.w.WriteByte(byte(v >> 16))
	b.w.WriteByte(byte(v >> 24))
	b.w.WriteByte(byte(v >> 32))
	b.w.WriteByte(byte(v >> 40))
	b.w.WriteByte(byte(v >> 48))
	b.w.WriteByte(byte(v >> 56))

	l := len(a.Value())

	b.w.WriteByte(byte(l))
	b.w.WriteByte(byte(l >> 8))
	b.w.WriteByte(byte(l >> 16))
	b.w.WriteByte(byte(l >> 24))

	b.w.Write(a.Value())
}

func (b *FBuffer) doneWriting() {
	if b.unread == 0 {
		return
	}
	if b.r == nil {
		b.w.Flush()
		//b.gzw.Close()
		// gz, err := gzip.NewReader(b.buffer)
		// if err != nil {
		// 	panic(err)
		// }
		// b.r = bufio.NewReaderSize(gz, 1 << 18)
		//b.r = bufio.NewReaderSize(b.buffer, 1 << 18)
		b.r = bufio.NewReaderSize(b.buffer, 4096)
	}
}

func (b *FBuffer) peek() Item {
	b.doneWriting()

	if b.unread == 0 {
		return nil
	}
	
	tb, err := b.r.Peek(12)
	if err != nil {
		panic(err)
	}
	key := binary.LittleEndian.Uint64(tb[0:8])

        l := binary.LittleEndian.Uint32(tb[8:12])

	tb, err = b.r.Peek(int(12+l))
	if err != nil {
		panic(err)
	}
	value := tb[12:12+l]
	//value := make([]byte, l)
	//copy(value, tb[12:12+l])

	item := NewItem(int64(key), value)
	return item
}

func (b *FBuffer) Read() Item {
	b.doneWriting()

	if b.unread == 0 {
		return nil
	}
 
	n, err := io.ReadFull(b.r, tb[0:8])
	if n!=8 || err != nil {
		panic(err)
	}
	key := binary.LittleEndian.Uint64(tb[0:8])

	n, err = io.ReadFull(b.r, tb[0:4])
	if n!=4 || err != nil {
		panic(err)
	}
        l := binary.LittleEndian.Uint32(tb[0:4])

	value := make([]byte, l)
	n, err = io.ReadFull(b.r, value)
	if n!=int(l) || err != nil {
		panic(err)
	}

	item := NewItem(int64(key), value)
	b.unread -= 1
	return item
}
