package funnelsort

import (
	"io/ioutil"
	"log"
	"os"
	"syscall"
)

type Buffer interface {
	Reader
	Writer
	peek() Item
	empty() bool
	full() bool
	reset()
	Close()
	SubBuffer(offset, max uint64) Buffer
}

func NewBuffer(n uint64) *MMBuffer {
	t := uint64(SIZE) * n
	map_file, err := ioutil.TempFile("/tmp/", "lfs")
	if err != nil {
		panic(err)
	}
	_, err = map_file.Seek(int64(t-1), 0)
	if err != nil {
		panic(err)
	}
	_, err = map_file.Write([]byte(" "))
	if err != nil {
		panic(err)
	}
	return &MMBuffer{max: n, map_file: map_file}
}

type MMBuffer struct {
	current_block           int
	_data                   []byte
	map_file                *os.File
	offset, head, tail, max uint64
}

func (b *MMBuffer) data(block int) []byte {
	if block+1 != b.current_block {
		if b.current_block != 0 {
			err := syscall.Munmap(b._data)
			if err != nil {
				panic(err)
			}
		}
		if block >= 0 {
			offset := int64(block) * int64(BLOCK_SIZE)
			mmap, err := syscall.Mmap(int(b.map_file.Fd()), offset, BLOCK_SIZE, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
			if err != nil {
				panic(err)
			}
			b._data = mmap
		} else {
			b._data = []byte{}
		}
		b.current_block = block + 1
	}
	return b._data
}

func (b *MMBuffer) Close() {
	b.data(-1)
	err := b.map_file.Close()
	if err != nil {
		log.Println(err)
	}
	err = os.Remove(b.map_file.Name())
	if err != nil {
		log.Println(err)
	}
}

func (b *MMBuffer) SubBuffer(offset, max uint64) Buffer {
	return &MMBuffer{offset: offset, max: max, map_file: b.map_file}
}

var SIZE int
var BLOCK_SIZE int

func SetSize(size int) {
	SIZE = size
	BLOCK_SIZE = (1<<16) * size //4096 * size
}

func fromIndex(n uint64) (int, int) {
	i := n * uint64(SIZE)
	return int(i / uint64(BLOCK_SIZE)), int(i % uint64(BLOCK_SIZE))
}

func (b *MMBuffer) Unread() uint64 {
	return b.tail - b.head
}

func (b *MMBuffer) empty() bool {
	return b.tail == b.head
}

func (b *MMBuffer) full() bool {
	return b.tail == b.max
}

func (b *MMBuffer) peek() Item {
	block_number, i := fromIndex(b.offset + b.head)
	item := NewItem()
	item.Read(b.data(block_number)[i : i+SIZE])
	return item
}

func (b *MMBuffer) Read() Item {
	block_number, i := fromIndex(b.offset + b.head)
	b.head += 1
	item := NewItem()
	item.Read(b.data(block_number)[i : i+SIZE])
	return item
}

func (b *MMBuffer) reset() {
	b.head = 0
	b.tail = 0
}

func (b *MMBuffer) Write(a Item) {
	block_number, i := fromIndex(b.offset + b.tail)
	a.Write(b.data(block_number)[i:i+SIZE])
	b.tail += 1
}

type ChBuffer struct {
	ch        chan Item
	unread    uint64
	hasPeek   bool
	peekValue Item
}

func NewChBuffer(n uint64) Buffer {
	return &ChBuffer{ch: make(chan Item, n)}
}

func (b *ChBuffer) Write(item Item) {
	b.hasPeek = false
	b.unread += 1
	b.ch <- item
}

func (b *ChBuffer) Close() {
	close(b.ch)
}

func (b *ChBuffer) SubBuffer(offset, max uint64) Buffer {
	panic("")
}

func (b *ChBuffer) Unread() uint64 {
	return b.unread
}

func (b *ChBuffer) empty() bool {
	return b.unread == 0 && b.hasPeek == false
}

func (b *ChBuffer) full() bool {
	return b.unread == uint64(cap(b.ch))
}

func (b *ChBuffer) peek() Item {
	if b.hasPeek == false {
		b.peekValue = b.Read()
		b.hasPeek = true
	}
	return b.peekValue
}

func (b *ChBuffer) Read() Item {
	if b.hasPeek {
		b.hasPeek = false
		return b.peekValue
	}
	b.unread -= 1
	return <-b.ch
}

func (b *ChBuffer) reset() {
	if !b.empty() {
		panic("")
	}
}
