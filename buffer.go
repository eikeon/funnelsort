package funnelsort

type Buffer interface {
	Reader
	Writer
	Unread() uint64 // TODO: big.Int ?
	peek() Item
	empty() bool
	full() bool
	reset()
	Close()
}

func NewFBuffer(n uint64) *FBuffer {
	b := &FBuffer{max: n}
	b.buffer = make([]byte, 0, 1 << 10)
	b.reset()
	return b
}

type FBuffer struct {
	max, unread uint64
	buffer      []byte
	off       int     // read at &buf[off], write at &buf[len(buf)]
}

func (b *FBuffer) Close() {
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
	b.buffer = b.buffer[0:0]
	b.off = 0
}

func (b *FBuffer) Write(a Item) {
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

func (b *FBuffer) peek() Item {
	if b.unread == 0 {
		return nil
	}
	return NewItem(b.buffer[b.off:])
}

func (b *FBuffer) Read() Item {
	if b.unread == 0 {
		return nil
	}
	b.unread -= 1
	item := NewItem(b.buffer[b.off:])
	b.off += len(item.Bytes())
	return item
}

func (b *FBuffer) grow(n int) int {
	m := len(b.buffer)
	if len(b.buffer)+n > cap(b.buffer) {
		buf := make([]byte, 2*cap(b.buffer) + n)
		copy(buf[b.off:], b.buffer[b.off:])
		b.buffer = buf
	}
	b.buffer = b.buffer[0 : m+n]
	return b.off
}
