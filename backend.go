package whiskey

// newbackend will return a new Mmap
func newbackend(a *allocator) *backend {
	var b backend
	b.a = a
	b.offset = -1
	b.sz = -1
	return &b
}

// backend manages the memory mapped file
type backend struct {
	a      *allocator
	bs     []byte
	offset int64
	sz     int64
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

	if bs, offset, grew = b.a.allocate(sz); grew && b.offset != -1 {
		b.bs = b.a.mm[b.offset:]
	}

	if b.offset != -1 {
		// Copy old bytes to new byteslice
		copy(bs, b.bs)
		// Release old bytes to allocator
		b.a.release(b.offset, b.sz)
	}

	// Update byteslice reference
	b.bs = bs
	// Update offset
	b.offset = offset
	// Update size
	b.sz = int64(len(bs))
	return
}

func (b *backend) dup() (out *backend) {
	out = newbackend(b.a)
	out.Grow(b.sz)
	copy(out.bs, b.bs)
	return
}

// Close will close an backend
func (b *backend) Close() (err error) {
	// Release old bytes to allocator
	b.a.release(b.offset, b.sz)
	b.a = nil
	b.bs = nil
	b.offset = -1
	b.sz = -1
	return
}

func next32(val int64) int64 {
	rem := val % 32
	if rem == 0 {
		return val
	}

	return val + (32 - rem)
}
