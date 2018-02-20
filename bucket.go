package whiskey

import "github.com/itsmontoya/rbt"

const (
	bucketInitSize = 256
)

func newBucket(key []byte, rgfn, sgfn GrowFn) *Bucket {
	var b Bucket
	// Because the provided key is a reference to the DB's key buffer, we will need to copy
	// the contents into a new slice so that we don't encounter any race conditions later

	// Make a new byteslice with the length of the provided key
	b.key = make([]byte, len(key))
	// Copy key buffer to key
	copy(b.key, key)

	if rgfn != nil {
		b.rgfn = rgfn
		b.r, _ = rbt.NewRaw(bucketInitSize, b.growRoot, nil)
	}

	if sgfn != nil {
		b.sgfn = sgfn
		b.w, _ = rbt.NewRaw(bucketInitSize, b.growScratch, nil)
	}

	return &b
}

// Bucket represents a database bucket
type Bucket struct {
	Txn

	key []byte

	rgfn GrowFn
	sgfn GrowFn
}

func (b *Bucket) growRoot(sz int64) (bs []byte) {
	if b.rgfn == nil {
		panic("root is attempting to grow past it's intended size")
	}

	return b.rgfn(b.key, sz)
}

func (b *Bucket) growScratch(sz int64) (bs []byte) {
	if b.sgfn == nil {
		panic("scratch is attempting to grow past it's intended size")
	}

	return b.sgfn(b.key, sz)
}

// NOTE: Trying to clean up how buckets allocate themselves

// Close will close a bucket
func (b *Bucket) Close() (err error) {
	if err = b.w.Close(); err != nil {
		return
	}

	// Technically we will panic if close is called twice.
	// Let's take some time to really plan this and see how we
	// want to approach this
	b.w = nil

	b.key = nil
	b.r = nil
	b.w = nil
	b.rgfn = nil
	b.sgfn = nil
	return
}

// GrowFn is called on grows
type GrowFn func(key []byte, sz int64) []byte
