// Package funnelsort implements lazy funnel sort, a cache-oblivious
// sorting algorithm.
//
//     http://courses.csail.mit.edu/6.851/spring12/lectures/L09.html
//     http://www.cs.amherst.edu/~ccm/cs34/papers/a2_2-brodal.pdf
//
package funnelsort

import (
	"math"
	"sort"
	"syscall"
)

type Item interface {
	Less(b Item) bool
	Bytes() []byte
}

// Function funnelsort uses to create a new item from a slice of
// bytes. The function must be set before calling FunnelSort.
var NewItem func(b []byte) Item

// The maximum length of an item in bytes.
var MaxItemLength = 4096

type Reader interface {
	Read() Item
}

type Writer interface {
	Write(i Item)
}

type Buffer interface {
	Reader
	Writer
	Peek() Item
	Empty() bool
	Full() bool
	Reset()
	Close()
}

type MBuffer struct {
	unread uint64
	buffer []byte
	off    int
}

func (b *MBuffer) Close() {
	b.unmap()
}

func (b *MBuffer) Empty() bool {
	return b.unread == 0
}

func (b *MBuffer) Full() bool {
	return len(b.buffer)+MaxItemLength >= cap(b.buffer)
}

func (b *MBuffer) Reset() {
	b.unread = 0
	b.buffer = b.buffer[0:0]
	b.off = 0
}

func (b *MBuffer) Write(a Item) {
	b.unread += 1
	// The following must hold: len(b.buffer) + len(a.Bytes()) <
	// cap(b.buffer) which is currently ensured via MaxItemLength
	b.buffer = append(b.buffer, a.Bytes()...)
}

func (b *MBuffer) Peek() (item Item) {
	if b.unread > 0 {
		item = NewItem(b.buffer[b.off:])
	}
	return
}

func (b *MBuffer) Read() (item Item) {
	if b.unread > 0 {
		b.unread -= 1
		item = NewItem(b.buffer[b.off:])
		b.off += len(item.Bytes())
	}
	return item
}

func (b *MBuffer) unmap() {
	if cap(b.buffer) > 0 {
		if err := syscall.Munmap(b.buffer[0:cap(b.buffer)]); err == nil {
			b.buffer = nil
		} else {
			panic(err)
		}
	}
}

func NewMBuffer(capacity int) (buffer *MBuffer) {
	if mmap, err := syscall.Mmap(-1, 0, capacity, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE); err == nil {
		buffer = &MBuffer{buffer: mmap[0:0]}
	} else {
		panic(err)
	}
	return
}

type MultiBuffer struct {
	max, unread uint64
	buffers     []Buffer
	read, write int
}

func (mb *MultiBuffer) Empty() bool {
	return mb.unread == 0
}

func (mb *MultiBuffer) Full() bool {
	return mb.max != 0 && mb.unread == mb.max
}

func (mb *MultiBuffer) Reset() {
	mb.unread = 0
	mb.read = 0
	mb.write = 0
	for _, b := range mb.buffers {
		b.Reset()
	}
}

func (mb *MultiBuffer) getBuffer(i int) Buffer {
	n := len(mb.buffers)
	if i < n {
		return mb.buffers[i]
	} else if i == n {
		b := NewMBuffer(1 << 28)
		mb.buffers = append(mb.buffers, b)
		return b
	}
	panic("tried to get buffer out of range")
}

func (mb *MultiBuffer) Write(a Item) {
	mb.unread += 1
	for w := mb.write; ; w++ {
		c := mb.getBuffer(w)
		if c.Full() == false {
			mb.write = w
			c.Write(a)
			break
		}
	}
}

func (mb *MultiBuffer) getReadBuffer() (buffer Buffer) {
	for r, n := mb.read, len(mb.buffers); r < n; r++ {
		c := mb.getBuffer(r)
		if c.Empty() == false {
			buffer = c
			mb.read = r
			break
		}
	}
	return
}

func (mb *MultiBuffer) Peek() (item Item) {
	if mb.unread > 0 {
		if c := mb.getReadBuffer(); c != nil {
			item = c.Peek()
		}
	}
	return
}

func (mb *MultiBuffer) Read() (item Item) {
	if mb.unread > 0 {
		mb.unread -= 1
		if c := mb.getReadBuffer(); c != nil {
			item = c.Read()
		}
	}
	return
}

func (mb *MultiBuffer) Close() {
	for _, b := range mb.buffers {
		b.Close()
	}
	mb.buffers = nil
}

func NewBuffer() Buffer {
	return &MultiBuffer{}
}

func NewBufferSize(n uint64) Buffer {
	return &MultiBuffer{max: n}
}

func hyperceil(x float64) uint64 {
	return uint64(math.Exp2(math.Ceil(math.Log2(x))))
}

type Funnel struct {
	height      uint64
	index       int
	exhausted   bool
	out         Buffer
	left, right *Funnel
	top         *Funnel
	bottom      []*Funnel
}

func (f *Funnel) K() uint64 {
	return uint64(math.Exp2(float64(f.height)))
}

func (f *Funnel) root() (root *Funnel) {
	if f.top == nil {
		root = f
	} else {
		root = f.top.root()
	}
	return root
}

func (f *Funnel) attach(funnel *Funnel, i int) {
	if funnel.left == nil && funnel.right == nil {
		funnel.left = f.bottom[2*i].root()
		funnel.right = f.bottom[2*i+1].root()
	} else {
		f.attach(funnel.left, i<<1)
		f.attach(funnel.right, i<<1+1)
	}
}

func (f *Funnel) addIndex(funnel *Funnel, i int) {
	if funnel.left == nil && funnel.right == nil {
		funnel.index = i
	} else {
		f.addIndex(funnel.left, i<<1)
		f.addIndex(funnel.right, i<<1+1)
	}
}

func (f *Funnel) attachInput(in []Buffer, i int) {
	if f.left == nil && f.right == nil {
		f.left = &Funnel{out: in[2*i], exhausted: true}
		f.right = &Funnel{out: in[2*i+1], exhausted: true}
	} else {
		f.left.attachInput(in, i<<1)
		f.right.attachInput(in, i<<1+1)
	}
}

func (f *Funnel) fill(out Writer) {
	bout, ok := out.(Buffer)
	if ok {
		bout.Reset()
	}
	for bout == nil || bout.Full() == false {
		if f.left.exhausted == false && f.left.out.Empty() {
			f.left.fill(f.left.out)
		}
		if f.right.exhausted == false && f.right.out.Empty() {
			f.right.fill(f.right.out)
		}
		if f.left.out.Empty() {
			if f.right.out.Empty() {
				f.left.out.Close()
				f.right.out.Close()
				f.exhausted = true
				break
			} else {
				out.Write(f.right.out.Read())
			}
		} else {
			if f.right.out.Empty() {
				out.Write(f.left.out.Read())
			} else {
				if f.left.out.Peek().Less(f.right.out.Peek()) {
					out.Write(f.left.out.Read())
				} else {
					out.Write(f.right.out.Read())
				}
			}
		}
	}
}

func (f *Funnel) Fill(in []Buffer, out Writer) {
	root := f.root()
	root.attachInput(in, 0)
	root.fill(out)
}

func (f *Funnel) Close() {
	if f.top != nil {
		f.top.Close()
	}
	for _, b := range f.bottom {
		b.Close()
	}
	if f.out != nil {
		f.out.Close()
	}
	if f.left != nil {
		f.left.Close()
	}
	if f.right != nil {
		f.right.Close()
	}
}

func NewFunnelK(k int) *Funnel {
	kk := uint64(hyperceil(float64(k)))
	h := uint64(math.Ceil(math.Log2(float64(kk))))
	return NewFunnel(h)
}

func NewFunnel(height uint64) *Funnel {
	f := &Funnel{height: height}
	if height > 1 {
		heightBottom := hyperceil(float64(height) / 2.)
		heightTop := height - heightBottom
		f.top = NewFunnel(heightTop)
		k := f.top.K()
		f.bottom = make([]*Funnel, k)
		bsize := uint64(math.Ceil(math.Pow(float64(k), 1.5)))
		f.top.out = NewBufferSize(bsize)
		for i, _ := range f.bottom {
			f.bottom[i] = NewFunnel(heightBottom)
			f.bottom[i].out = NewBufferSize(bsize)
		}
		f.attach(f.top.root(), 0)
	}
	f.addIndex(f.root(), 0)
	return f
}

type itemSlice []Item

func (p itemSlice) Len() int           { return len(p) }
func (p itemSlice) Less(i, j int) bool { return p[i].Less(p[j]) }
func (p itemSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type itemBuffer struct {
	buf itemSlice
}

func (p *itemBuffer) Close() {
}

func (p *itemBuffer) Empty() bool {
	return len(p.buf) == 0
}

func (p *itemBuffer) Full() bool {
	return false
}

func (p *itemBuffer) Reset() {
	p.buf = p.buf[0:0]
}

func (p *itemBuffer) Write(a Item) {
	p.buf = append(p.buf, a)
}

func (p *itemBuffer) Peek() (item Item) {
	if len(p.buf) > 0 {
		item = p.buf[0]
	}
	return
}

func (p *itemBuffer) Read() (item Item) {
	if len(p.buf) > 0 {
		item = p.buf[0]
		p.buf = p.buf[1:]
	}
	return
}

const p = uint(16)
const kSquared = 1 << p

var itemArray [kSquared]Item

func manual(in Reader) (items itemSlice, done bool) {
	items = itemSlice(itemArray[0:0])
	for i := 0; i < kSquared; i++ {
		if item := in.Read(); item != nil {
			items = append(items, item)
		} else {
			done = true
			break
		}
	}
	sort.Sort(items)
	return
}

var empty = &itemBuffer{make(itemSlice, 0)}

func Merge(buffers []Buffer, out Writer) {
	if len(buffers) == 1 {
		in := buffers[0]
		for {
			item := in.Read();
			if item == nil {
				break
			} else {
				out.Write(item)
			}
		}
	} else if len(buffers) > 0 {
		f := NewFunnelK(len(buffers))

		// pad the remaining inputs with an empty buffer
		for k := int(f.K()); len(buffers) < k; {
			buffers = append(buffers, empty)
		}
		f.Fill(buffers, out)
		f.Close()
		for _, b := range buffers {
			b.Close()
		}
	}
	return
}

func FunnelSort(in Reader, out Writer) {
	items, done := manual(in)
	if done {
		for _, item := range items {
			out.Write(item)
		}
		return
	}

	var buffers []Buffer
	for kMax := 1 << (p / 2); ; {
		for len(buffers) < kMax {
			buffer := NewBuffer()
			for _, item := range items {
				buffer.Write(item)
			}
			buffers = append(buffers, buffer)
			if done {
				break
			} else {
				items, done = manual(in)
			}
		}
		if done {
			Merge(buffers, out)
			break
		} else {
			buffer := NewBuffer()
			Merge(buffers, buffer)
			buffers = buffers[0:0]
			buffers = append(buffers, buffer)
			kMax += (1 << (p / 2))
		}
	}
}
