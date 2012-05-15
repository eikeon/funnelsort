package funnelsort

import (
	"bytes"
	"io"
)

type mBuffer interface {
	io.Reader
	io.Writer
	Len() int
}

type LargeBuffer struct {
	buffers []mBuffer
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
		if c, ok := current.(io.Closer); ok {
			c.Close()
		}
		mr.buffers = mr.buffers[1:]
	}
	return 0, io.EOF
}

const MAX_BUFFER_SIZE = (1 << 30)

func (mr *LargeBuffer) addBuffer() (buffer mBuffer) {
	if false { // TODO
		buffer = bytes.NewBuffer(make([]byte, MAX_BUFFER_SIZE)[0:0])
	} else {
		buffer = NewMMBuffer()
	}
	mr.buffers = append(mr.buffers, buffer)
	return
}

func (mr *LargeBuffer) Write(p []byte) (n int, err error) {
	if len(mr.buffers) == 0 {
		mr.addBuffer()
	}
	current := mr.buffers[len(mr.buffers)-1]

	if current.Len() >= MAX_BUFFER_SIZE {
		current = mr.addBuffer()
	}

	if len(p) > MAX_BUFFER_SIZE {
		written := 0
		for written+MAX_BUFFER_SIZE < len(p) {
			nn, _ := current.Write(p[written:written+MAX_BUFFER_SIZE])
			written += nn
			current = mr.addBuffer()
		}
		remaining := len(p) - written
		current.Write(p[written:written+remaining])
		return len(p), nil
	}
	return current.Write(p)
}

func (mr *LargeBuffer) Close() error {
	for _, b := range mr.buffers {
		if c, ok := b.(io.Closer); ok {
			c.Close()
		}
	}
	mr.buffers = nil
	return nil
}

func NewLargeBuffer() *LargeBuffer {
	lb := &LargeBuffer{}
	return lb
}
