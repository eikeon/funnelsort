package funnelsort

func NewBuffer(n uint64) Buffer {
	b := &MultiBuffer{max: n}	
	return b
}

type MultiBuffer struct {
	max, unread uint64
	buffers []Buffer
	read, write int
}

func (mb *MultiBuffer) Unread() uint64 {
	return mb.unread
}

func (mb *MultiBuffer) empty() bool {
	return mb.unread == 0
}

func (mb *MultiBuffer) full() bool {
	return mb.unread == mb.max
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
		b := NewMBuffer(1 << 18)
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
	c.Write(a)
}

func (mb *MultiBuffer) getReadBuffer() Buffer {
	r := mb.read
	c := mb.getBuffer(r)
	n := len(mb.buffers)
	for ; c.empty(); r++ {
		if r < n {
			c = mb.getBuffer(r)		
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
