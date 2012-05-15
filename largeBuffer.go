package funnelsort

import (
	"bytes"
	"io"
)

type LargeBuffer struct {
	buffers []*bytes.Buffer
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
		mr.buffers = mr.buffers[1:]
	}
	return 0, io.EOF
}

const MAX_BUFFER_SIZE = (1 << 14)

func (mr *LargeBuffer) addBuffer() *bytes.Buffer {
	current := bytes.NewBuffer(make([]byte, MAX_BUFFER_SIZE)[0:0])
	mr.buffers = append(mr.buffers, current)
	return current
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

func (mr *LargeBuffer) Close() {
	mr.buffers = nil
}

func NewLargeBuffer() *LargeBuffer {
	lb := &LargeBuffer{}
	return lb
}
