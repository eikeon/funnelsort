package funnelsort

type OBuffer struct {
	out Writer
}

func (b *OBuffer) Close() {
}

func (b *OBuffer) Unread() uint64 {
	panic("")
}

func (b *OBuffer) empty() bool {
	panic("")
}

func (b *OBuffer) full() bool {
	return false
}

func (b *OBuffer) reset() {
}

func (b *OBuffer) Write(a Item) {
	b.out.Write(a)
}

func (b *OBuffer) peek() Item {
	panic("")
}

func (b *OBuffer) Read() Item {
	panic("")
}
