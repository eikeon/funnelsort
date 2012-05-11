package funnelsort

// A mmap backed buffer for large buffers

import (
	"io"
	"io/ioutil"
	"os"
	"syscall"
)

type MMBuffer struct {
	file *os.File
	buf  []byte
	off  int
	mmap []byte
}

func (b *MMBuffer) Len() int { return len(b.buf) - b.off }

func (b *MMBuffer) Reset() {
	b.off = 0
	b.buf = b.buf[0:0]
}

// grow grows the buffer to guarantee space for n more bytes.
// It returns the index where bytes should be written.
func (b *MMBuffer) grow(n int) int {
	m := b.Len()
	// If buffer is empty, reset to recover space.
	if m == 0 && b.off != 0 {
		b.Reset()
	}
	if len(b.buf)+n > cap(b.buf) {
		b.buf = b.Map(2*cap(b.buf) + n)[0:0]
		b.off = 0
	}
	b.buf = b.buf[0 : b.off+m+n]
	return b.off + m
}

func (b *MMBuffer) Write(p []byte) (n int, err error) {
	m := b.grow(len(p))
	return copy(b.buf[m:], p), nil
}

func (b *MMBuffer) Read(p []byte) (n int, err error) {
	if b.off >= len(b.buf) {
		// MMBuffer is empty, reset to recover space.
		b.Reset()
		if len(p) == 0 {
			return
		}
		return 0, io.EOF
	}
	n = copy(p, b.buf[b.off:])
	b.off += n
	return
}

func (b *MMBuffer) unmap() {
	if len(b.mmap) > 0 {
		err := syscall.Munmap(b.mmap)
		if err != nil {
			panic(err)
		}
		b.mmap = []byte{}
	}
}

func (b *MMBuffer) Close() {
	b.unmap()
	b.file.Close()
	err := os.Remove(b.file.Name())
	if err != nil {
		//log.Println(err)
	}
}

func (b *MMBuffer) Map(capacity int) []byte {
	_, err := b.file.Seek(int64(capacity-1), 0)
	if err != nil {
		panic(err)
	}
	_, err = b.file.Write([]byte(" "))
	if err != nil {
		panic(err)
	}
	b.unmap()
	mmap, err := syscall.Mmap(int(b.file.Fd()), 0, capacity, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	return mmap
}

func NewMMBuffer() *MMBuffer {
	file, err := ioutil.TempFile("./", "tmpfunnelsort")
	if err != nil {
		panic(err)
	}
	return &MMBuffer{file: file}
}

type LargeBuffer struct {
	buffers []*MMBuffer
}

func (mr *LargeBuffer) Read(p []byte) (n int, err error) {
	for len(mr.buffers) > 0 {
		current := mr.buffers[0]
		n, err = current.Read(p)
		if n > 0 || err != io.EOF {
			if err == io.EOF {
				// Don't return io.EOF yet. There may be more bytes
				// in the remaining buffers.
				err = nil
			}
			return
		}
		current.Close()
		mr.buffers = mr.buffers[1:]
	}
	return 0, io.EOF
}

const MAX_BUFFER_SIZE = (1 << 28)

func (mr *LargeBuffer) Write(p []byte) (n int, err error) {
	current := mr.buffers[len(mr.buffers)-1]
	if len(current.buf)+len(p) > MAX_BUFFER_SIZE {
		current = NewMMBuffer()
		mr.buffers = append(mr.buffers, current)
	}
	return current.Write(p)
}

func (mr *LargeBuffer) Close() {
	for _, b := range mr.buffers {
		b.Close()
	}
}

func NewLargeBuffer() *LargeBuffer {
	lb := &LargeBuffer{}
	lb.buffers = append(lb.buffers, NewMMBuffer())
	return lb
}
