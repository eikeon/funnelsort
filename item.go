package funnelsort

//import "log"

type Item interface {
	Less(b Item) bool
	Key() int64
	Value() []byte
}

//var LessItem func(a, b Item) bool
var NewItem func(key int64, value []byte) Item

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
	return len(p.buf)==0
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
	//log.Println("read:", item)
	return
}
