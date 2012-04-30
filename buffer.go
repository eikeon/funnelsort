package funnel

import (
	"encoding/binary"
	"io/ioutil"
	"log"
	"os"
	"syscall"
	"unsafe"
)

type Buffer interface {
	read() uint64
	write(a uint64)
	empty() bool
	full() bool
	Length() uint64
	peek() uint64
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
	current_block   int
	_data           []byte
	map_file        *os.File
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

const SIZE = int(unsafe.Sizeof(uint64(0)))
const BLOCK_SIZE = 4096 * SIZE

func fromIndex(n uint64) (int, int) {
	i := n * uint64(SIZE)
	return int(i / uint64(BLOCK_SIZE)), int(i % uint64(BLOCK_SIZE))
}

func (b *MMBuffer) Length() uint64 {
	return b.tail - b.head
}

func (b *MMBuffer) empty() bool {
	return b.tail == b.head
}

func (b *MMBuffer) full() bool {
	return b.tail == b.max
}

func (b *MMBuffer) peek() uint64 {
	block_number, i := fromIndex(b.offset + b.head)
	return binary.LittleEndian.Uint64(b.data(block_number)[i : i+SIZE])
}

func (b *MMBuffer) read() uint64 {
	block_number, i := fromIndex(b.offset + b.head)
	b.head += 1
	return binary.LittleEndian.Uint64(b.data(block_number)[i : i+SIZE])
}

func (b *MMBuffer) reset() {
	b.head = 0
	b.tail = 0
}

func (b *MMBuffer) write(a uint64) {
	block_number, i := fromIndex(b.offset + b.tail)
	binary.LittleEndian.PutUint64(b.data(block_number)[i:i+SIZE], a)
	b.tail += 1
}

