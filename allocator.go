package whiskey

import (
	"os"
	"path"

	mmap "github.com/edsrzf/mmap-go"
	"github.com/missionMeteora/journaler"
	"github.com/missionMeteora/toolkit/errors"
)

// RW represents Read-Write permissions
const RW = os.O_CREATE | os.O_RDWR

// ROnly represents Read-only permissions
const ROnly = os.O_RDONLY

// newallocator will return a new Mmap
func newallocator(dir, name string, perms int) (ap *allocator, err error) {
	var a allocator
	if a.f, err = os.OpenFile(path.Join(dir, name), perms, 0644); err != nil {
		return
	}

	ap = &a
	return
}

// allocator manages the memory mapped file
type allocator struct {
	f  *os.File
	mm mmap.MMap

	tail int64
	cap  int64
}

func (a *allocator) unmap() (err error) {
	if a.mm == nil {
		return
	}

	return a.mm.Unmap()
}

func (a *allocator) grow(sz int64) {
	var err error
	if a.cap == 0 {
		var fi os.FileInfo
		if fi, err = a.f.Stat(); err != nil {
			journaler.Error("Stat error: %v", err)
			return
		}

		if a.cap = fi.Size(); a.cap == 0 {
			a.cap = sz
		}
	}

	for a.cap < sz {
		a.cap *= 2
	}

	if err = a.unmap(); err != nil {
		journaler.Error("Unmap error: %v", err)
		return
	}

	if err = a.f.Truncate(a.cap); err != nil {
		journaler.Error("Truncate error: %v", err)
		return
	}

	if a.mm, err = mmap.Map(a.f, os.O_RDWR, 0); err != nil {
		journaler.Error("Map error: %v", err)
		return
	}
}

func (a *allocator) allocate(sz int64) (bs []byte, offset int64, grew bool) {
	offset = a.tail
	if a.tail += sz; a.cap <= a.tail {
		a.grow(a.tail)
		grew = true
	}

	bs = a.mm[offset:a.tail]
	return
}

func (a *allocator) release(offset, sz int64) {

}

// Close will close an allocator
func (a *allocator) Close() (err error) {
	if a.f == nil {
		return errors.ErrIsClosed
	}

	var errs errors.ErrorList
	errs.Push(a.mm.Flush())
	errs.Push(a.mm.Unmap())
	errs.Push(a.f.Close())
	a.f = nil
	return
}
