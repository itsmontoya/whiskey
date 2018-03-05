package whiskey

import "sort"

type freelist struct {
	s []*pair
}

func (f *freelist) acquire(sz int64) (offset int64) {
	for i, p := range f.s {
		switch {
		case p.sz < sz:
			// Pair size is smaller than requested size, continue
			continue
		case p.sz == sz:
			// Pair size is the same as the requested size
			// Set offset as pair
			offset = p.offset
			// Remove pair from freelist
			f.remove(i)
		case p.sz > sz:
			// Pair size is bigger than requested size,
			// Set offset as pair
			offset = p.offset
			// Move pair's offset up by the size amount
			p.offset += sz
			// Reduce pair's size by the size amount
			p.sz -= sz
		}

		return
	}

	return -1
}

func (f *freelist) release(offset, sz int64) {
	f.append(offset, sz)
	f.sort()
	f.merge()
}

func (f *freelist) append(offset, sz int64) {
	var p pair
	p.offset = offset
	p.sz = sz
	f.s = append(f.s, &p)
}

func (f *freelist) sort() {
	sort.Slice(f.s, f.isLess)
}

func (f *freelist) isLess(i, j int) bool {
	return f.s[i].offset < f.s[j].offset
}

func (f *freelist) merge() {
	var (
		last    *pair
		removed int
	)

	for i, p := range f.s {
		if last == nil || (p.offset != last.offset+last.sz) {
			last = p
			continue
		}

		last.sz += p.sz
		f.remove(i - removed)
		removed++
	}
}

func (f *freelist) remove(i int) {
	f.s = append(f.s[:i], f.s[i+1:]...)
}
