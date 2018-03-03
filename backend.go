package whiskey

// newbackend will return a new Mmap
func newbackend(a *allocator) *backend {
	var b backend
	b.a = a
	return &b
}

type pair struct {
	offset int64
	sz     int64
}

// backend manages the memory mapped file
type backend struct {
	a *allocator
	p pair

	bs []byte
}

func (b *backend) setBytes() {
	b.bs = b.a.mm[b.p.offset : b.p.offset+b.p.sz]
}

func (b *backend) Grow(sz int64) (bs []byte) {
	cap := int64(len(b.bs))
	if sz <= cap {
		return b.bs
	} else if cap == 0 {
		cap = next32(sz)
	}

	var (
		offset int64
		grew   bool
	)

	if bs, offset, grew = b.a.allocate(sz); grew {
		b.setBytes()
	}

	if b.p.offset != -1 {
		// Copy old bytes to new byteslice
		copy(bs, b.bs)
		// Release old bytes to allocator
		b.a.release(b.p.offset, b.p.sz)
	}

	// Update byteslice reference
	b.bs = bs
	// Update offset
	b.p.offset = offset
	// Update size
	b.p.sz = int64(len(bs))
	return
}

func (b *backend) dup() (out *backend) {
	out = newbackend(b.a)
	out.Grow(b.p.sz)
	b.setBytes()
	copy(out.bs, b.bs)
	return
}

// Close will close an backend
func (b *backend) Close() (err error) {
	// Release old bytes to allocator
	b.a.release(b.p.offset, b.p.sz)
	b.a = nil
	b.bs = nil
	return
}

func next32(val int64) int64 {
	rem := val % 32
	if rem == 0 {
		return val
	}

	return val + (32 - rem)
}
