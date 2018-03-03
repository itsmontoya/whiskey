package whiskey

const (
	bucketInitSize = 256
)

func newBucket(key []byte, gfn GrowFn) *Bucket {
	var b Bucket
	// Because the provided key is a reference to the DB's key buffer, we will need to copy
	// the contents into a new slice so that we don't encounter any race conditions later

	// Make a new byteslice with the length of the provided key
	b.key = make([]byte, len(key))
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
