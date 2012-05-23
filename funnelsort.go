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

type Reader interface {
	Read() Item
}

type Writer interface {
	Write(i Item)
}

type Buffer interface {
	Reader
	Writer
	Unread() uint64
	peek() Item
	empty() bool
	full() bool
	reset()
	Close()
}

var NewItem func(b []byte) Item

type MBuffer struct {
	max, unread uint64
	buffer      []byte
	off         int
	mmap        []byte
}

func (b *MBuffer) Close() {
	b.unmap()
}

func (b *MBuffer) Unread() uint64 {
	return b.unread
}

func (b *MBuffer) empty() bool {
	return b.unread == 0
}

func (b *MBuffer) full() bool {
	return len(b.buffer) + 4096 >= cap(b.mmap) // TODO: define max item length or somesuch
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
	b.buffer = b.buffer[0 : m+length]
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

func (b *MBuffer) unmap() {
	if len(b.mmap) > 0 {
		err := syscall.Munmap(b.mmap)
		if err != nil {
			panic(err)
		}
		b.mmap = nil
	}
}

func NewMBuffer(capacity int) *MBuffer {
	mmap, err := syscall.Mmap(-1, 0, capacity, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		panic(err)
	}
	return &MBuffer{mmap: mmap, buffer: mmap[0:0]}
}

type MultiBuffer struct {
	max, unread uint64
	buffers     []Buffer
	read, write int
}

func (mb *MultiBuffer) Unread() uint64 {
	return mb.unread
}

func (mb *MultiBuffer) empty() bool {
	return mb.unread == 0
}

func (mb *MultiBuffer) full() bool {
	return mb.max != 0 && mb.unread == mb.max
}

func (mb *MultiBuffer) reset() {
	mb.unread = 0
	mb.read = 0
	mb.write = 0
	for _, b := range mb.buffers {
		b.reset()
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
	panic("")
}

func (mb *MultiBuffer) Write(a Item) {
	mb.unread += 1
	w := mb.write
	c := mb.getBuffer(w)
	for ; c.full(); w++ {
		c = mb.getBuffer(w)
	}
	mb.write = w
	c.Write(a)
}

func (mb *MultiBuffer) getReadBuffer() Buffer {
	r := mb.read
	c := mb.getBuffer(r)
	n := len(mb.buffers)
	for ; c.empty(); r++ {
		if r < n {
			c = mb.getBuffer(r)
			mb.read = r
		} else {
			return nil
		}
	}
	return c
}
func (mb *MultiBuffer) peek() (item Item) {
	if mb.unread == 0 {
		return nil
	}
	if c := mb.getReadBuffer(); c != nil {
		item = c.peek()
	} else {
		item = nil
	}
	return
}

func (mb *MultiBuffer) Read() (item Item) {
	if mb.unread == 0 {
		return nil
	}
	mb.unread -= 1
	if c := mb.getReadBuffer(); c != nil {
		item = c.Read()
	} else {
		item = nil
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

func (f *Funnel) fill(in []Buffer, out Buffer) {
	out.reset()
	for out.full() == false {
		if f.left.exhausted == false && f.left.out.empty() {
			f.left.fill(in, f.left.out)
		}
		if f.right.exhausted == false && f.right.out.empty() {
			f.right.fill(in, f.right.out)
		}
		if f.left.out.empty() {
			if f.right.out.empty() {
				f.left.out.Close()
				f.right.out.Close()
				f.exhausted = true
				break
			} else {
				out.Write(f.right.out.Read())
			}
		} else {
			if f.right.out.empty() {
				out.Write(f.left.out.Read())
			} else {
				if f.left.out.peek().Less(f.right.out.peek()) {
					out.Write(f.left.out.Read())
				} else {
					out.Write(f.right.out.Read())
				}
			}
		}
	}
}

func (f *Funnel) Fill(in []Buffer, out Buffer) {
	root := f.root()
	root.attachInput(in, 0)
	root.fill(in, out)
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

func (p *itemBuffer) Unread() uint64 {
	return uint64(len(p.buf))
}

func (p *itemBuffer) empty() bool {
	return len(p.buf) == 0
}

func (p *itemBuffer) full() bool {
	return false
}

func (p *itemBuffer) reset() {
	p.buf = p.buf[0:0]
}

func (p *itemBuffer) Write(a Item) {
	p.buf = append(p.buf, a)
}

func (p *itemBuffer) peek() (item Item) {
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

func merge(buffers []Buffer, out Buffer) {
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
	return
}

type oBuffer struct {
	out Writer
}

func (b *oBuffer) Close() {
}

func (b *oBuffer) Unread() uint64 {
	panic("")
}

func (b *oBuffer) empty() bool {
	panic("")
}

func (b *oBuffer) full() bool {
	return false
}

func (b *oBuffer) reset() {
}

func (b *oBuffer) Write(a Item) {
	b.out.Write(a)
}

func (b *oBuffer) peek() Item {
	panic("")
}

func (b *oBuffer) Read() Item {
	panic("")
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
	kMax := 1 << (p / 2)
top:
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
		merge(buffers, &oBuffer{out})
	} else {
		buffer := NewBuffer()
		merge(buffers, buffer)
		buffers = buffers[0:0]
		buffers = append(buffers, buffer)
		kMax += (1 << (p / 2)) //kMax = int(math.Sqrt(float64(buffer.Unread())))
		goto top
	}
}
