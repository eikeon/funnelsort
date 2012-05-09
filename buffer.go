package funnelsort

import (
	"encoding/gob"
	"io"
	"io/ioutil"
	"os"
	"syscall"
	"unsafe"
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
	file, err := ioutil.TempFile("./", "tmpfunnelsort")
	if err != nil {
		panic(err)
	}
	b := &FBuffer{file: file, max: n}
	b.reset()
	return b
}

type FBuffer struct {
	max, unread uint64
	file        *os.File
	writer      io.WriteCloser
	enc         *gob.Encoder
	dec         *gob.Decoder
	reader      io.Reader
	peekItem    Item
	buf         [int(unsafe.Sizeof(uint64(0)))]byte
}

func (b *FBuffer) Close() {
	err := b.file.Close()
	if err != nil {
		//log.Println(err)
	}
	err = os.Remove(b.file.Name())
	if err != nil {
		//log.Println(err)
	}
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

func (b *FBuffer) reset() {
	b.unread = 0
	b.peekItem = nil
	b.reader = nil
	syscall.Ftruncate(int(b.file.Fd()), 0)
	b.file.Seek(0, 0)
	b.writer = b.file
	b.enc = gob.NewEncoder(b.writer)
}

func (b *FBuffer) Write(a Item) {
	b.unread += 1
	err := b.enc.Encode(a)
	if err != nil {
		panic(err)
	}
}

func (b *FBuffer) read() Item {
	if b.reader == nil {
		b.writer = nil
		b.file.Seek(0, 0)
		b.reader = b.file
		b.dec = gob.NewDecoder(b.reader)
	}
	item := NewItem()
	err := b.dec.Decode(item)
	if err != nil {
		panic(err)
	}
	return item
}
