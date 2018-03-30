package whiskey

const (
	bucketInitSize = 256
)

func newBucket(key []byte, t Txn) *Bucket {
	var b Bucket
	b.key = key
	b.Txn = t
	// Copy key buffer to key
	copy(b.key, key)
	return &b
}

// Bucket represents a database bucket
type Bucket struct {
	key []byte
	Txn
}

func (b *Bucket) getBucketKey(key []byte) (out []byte) {
	out = make([]byte, len(b.key)+len(key)+1)
	copy(out, b.key)
	b.key[len(b.key)] = bucketPrefix
	copy(out[len(b.key)+1:], key)
	return
}

func (b *Bucket) getKey(key []byte) (out []byte) {
	out = make([]byte, len(b.key)+len(key))
	copy(out, b.key)
	copy(out[len(b.key):], key)
	return
}

// CreateBucket will create a bucket
func (b *Bucket) CreateBucket(key []byte) (out *Bucket, err error) {
	return b.createBucket(b.getBucketKey(key))
}

// Bucket will retrieve a bucket
func (b *Bucket) Bucket(key []byte) (out *Bucket, err error) {
	return b.bucket(b.getBucketKey(key))
}

// Get will get by key
func (b *Bucket) Get(key []byte) (val []byte, err error) {
	if key[0] == bucketPrefix {
		return nil, ErrInvalidKey
	}

	return b.Txn.get(b.getKey(key))
}

// Put will set a value for a given key
func (b *Bucket) Put(key, val []byte) (err error) {
	if key[0] == bucketPrefix {
		return ErrInvalidKey
	}

	return b.Txn.put(b.getKey(key), val)
}

// Delete will delete by key
func (b *Bucket) Delete(key []byte) (err error) {
	if key[0] == bucketPrefix {
		return ErrInvalidKey
	}

	return b.Txn.delete(b.getKey(key))
}
