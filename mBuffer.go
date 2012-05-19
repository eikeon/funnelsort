package funnelsort

import (
	"io/ioutil"
	"os"
	"syscall"
)

func NewMBuffer(n uint64) *MBuffer {
	file, err := ioutil.TempFile("./", "tmpfunnelsort")
	if err != nil {
		panic(err)
	}
	return &MBuffer{max: n, file: file}
}

type MBuffer struct {
	max, unread uint64
	buffer      []byte
	file *os.File
	off  int
	mmap []byte
	mapped bool
}

func (b *MBuffer) Close() {
	b.unmap()
	b.file.Close()
	err := os.Remove(b.file.Name())
	if err != nil {
		//log.Println(err)
	}
}

func (b *MBuffer) Unread() uint64 {
	return b.unread
}

func (b *MBuffer) empty() bool {
	return b.unread == 0
}

func (b *MBuffer) full() bool {
	return b.unread == b.max
}

func (b *MBuffer) reset() {
	b.unread = 0
	b.buffer = b.buffer[0:0]
	b.off = 0
}

func (b *MBuffer) Write(a Item) {
	b.unread += 1
	v := a.Bytes()
	m := len(b.buffer)
	length := len(v)
	b.grow(length)
	
	b.buffer = b.buffer[0:m+length]
	mm := copy(b.buffer[m:m+length], v)
	if mm != length {
		panic("")
	}
}

func (b *MBuffer) peek() Item {
	if b.unread == 0 {
		return nil
	}
	return NewItem(b.buffer[b.off:])
}

func (b *MBuffer) Read() Item {
	if b.unread == 0 {
		return nil
	}
	b.unread -= 1
	item := NewItem(b.buffer[b.off:])
	b.off += len(item.Bytes())
	return item
}

// grow grows the buffer to guarantee space for n more bytes.
// It returns the index where bytes should be written.
func (b *MBuffer) grow(n int) int {
	m := len(b.buffer)
	if len(b.buffer)+n > cap(b.buffer) {
		b.buffer = b.Map(2*cap(b.buffer) + n)[0:0]
		b.off = 0
	}
	b.buffer = b.buffer[0 : b.off+m+n]
	return b.off + m
}

func (b *MBuffer) Map(capacity int) []byte {
	fi, err := b.file.Stat()
	if err != nil {
		panic(err)
	}
	if int64(capacity) > fi.Size() {
		_, err = b.file.Seek(int64(capacity-1), 0)
		if err != nil {
			panic(err)
		}
		_, err = b.file.Write([]byte(" "))
		if err != nil {
			panic(err)
		}
	}
	b.unmap()
	mmap, err := syscall.Mmap(int(b.file.Fd()), 0, capacity, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	b.mapped = true
	return mmap
}

func (b *MBuffer) unmap() {
	if len(b.mmap) > 0 {
		err := syscall.Munmap(b.mmap)
		if err != nil {
			panic(err)
		}
		b.mmap = []byte{}
		b.mapped = false
	}
}
