package funnelsort

type Item uint64

type ItemSlice []Item

func (p ItemSlice) Len() int           { return len(p) }
func (p ItemSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p ItemSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
