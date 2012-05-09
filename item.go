package funnelsort

type Item interface {
	Less(b Item) bool
}

//var LessItem func(a, b Item) bool
var NewItem func() Item

type itemSlice []Item

func (p itemSlice) Len() int           { return len(p) }
func (p itemSlice) Less(i, j int) bool { return p[i].Less(p[j]) }
func (p itemSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
