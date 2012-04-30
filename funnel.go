package funnel

import (
	"math"
)

func hyperceil(x float64) uint64 {
	return uint64(math.Exp2(math.Ceil(math.Log2(x))))
}

type Funnel struct {
	// number of inputs to the funnel
	// k           uint32 
	height      uint64

	index       int
	exhausted   bool
	out         Buffer
	left, right *Funnel
	top         *Funnel
	bottom      []*Funnel
	buffer      Buffer
}

func (f *Funnel) K() uint64 {
	return uint64(math.Exp2(float64(f.height)))
}

func (f *Funnel) Height() uint64 {
	return f.height
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
				f.exhausted = true
				break
			} else {
				out.write(f.right.out.read())
			}
		} else {
			if f.right.out.empty() {
				out.write(f.left.out.read())
			} else {
				if f.left.out.peek() < f.right.out.peek() {
					out.write(f.left.out.read())
				} else {
					out.write(f.right.out.read())
				}
			}
		}
	}
}

func NewFunnel(height uint64) *Funnel {  // TODO: switch from height to K?
	f := &Funnel{height: height}
	if height > 1 {
		heightBottom := hyperceil(float64(height) / 2.)
		heightTop := height - heightBottom
		f.top = NewFunnel(heightTop)
		k := f.top.K()
		f.bottom = make([]*Funnel, k)
		bsize := uint64(math.Ceil(math.Pow(float64(k), 1.5)))
		f.buffer = NewBuffer(uint64(bsize * (k+1)))
		f.top.out = f.buffer.SubBuffer(uint64(k)*bsize, bsize)
		for i, _ := range f.bottom {
			f.bottom[i] = NewFunnel(heightBottom)
			f.bottom[i].out = f.buffer.SubBuffer(uint64(i)*bsize, bsize)
		}
		f.attach(f.top.root(), 0)
	}
	f.addIndex(f.root(), 0)
	return f
}

func (f *Funnel) Fill(in []Buffer, out Buffer) {
	root := f.root()
	root.attachInput(in, 0)
	root.fill(in, out)
}

func (f *Funnel) Close() {
	if f.buffer != nil {
		f.buffer.Close()
	}
	if f.top != nil {
		f.top.Close()
	}
	//if f.out != nil {
	//	f.out.Close()
	//}
	if f.left != nil {
		f.left.Close()
	}
	if f.right != nil {
		f.right.Close()
	}
}
