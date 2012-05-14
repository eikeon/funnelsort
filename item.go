package funnelsort

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
