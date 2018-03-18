package whiskey

const (
	bucketInitSize = 256
)

func newBucket(key []byte, gfn GrowFn) *Bucket {
	var b Bucket
	b.key = key
	// Copy key buffer to key
	copy(b.key, key)
	b.gfn = gfn
	return &b
}

// Bucket represents a database bucket
type Bucket struct {
	key []byte
	gfn GrowFn
	Txn
}

func (b *Bucket) grow(sz int64) (bs []byte) {
	return b.gfn(b.key, sz)
}

// GrowFn is called on grows
type GrowFn func(key []byte, sz int64) []byte
