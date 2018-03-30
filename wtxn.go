package whiskey

import (
	"github.com/itsmontoya/rbt"
)

// WTxn is a transaction type
type WTxn struct {
	t *rbt.Tree
}

func (t *WTxn) newBucket(key []byte) (bp *Bucket) {
	return newBucket(key, t)
}

func (t *WTxn) bucket(key []byte) (bp *Bucket, err error) {
	var bs []byte
	if bs = t.t.Get(key); bs == nil {
		err = ErrKeyDoesNotExist
		return
	}

	bp = t.newBucket(key)
	return
}

// createBucket will create a bucket for a provided key
func (t *WTxn) createBucket(key []byte) (bp *Bucket, err error) {
	if bp, err = t.bucket(key); err == nil {
		return
	}

	t.t.Put(key, []byte{13})
	return t.newBucket(key), nil
}

func (t *WTxn) get(key []byte) (val []byte, err error) {
	val = t.t.Get(key)
	return
}

func (t *WTxn) put(key, val []byte) (err error) {
	t.t.Put(key, val)
	return
}

func (t *WTxn) delete(key []byte) (err error) {
	return ErrCannotWrite
}

// Bucket will return a bucket for a provided key
func (t *WTxn) Bucket(key []byte) (bp *Bucket, err error) {
	return t.bucket(getBucketKey(key))
}

// CreateBucket will create a bucket for a provided key
func (t *WTxn) CreateBucket(key []byte) (bp *Bucket, err error) {
	return t.createBucket(getBucketKey(key))
}

// Get will retrieve a value for a given key
func (t *WTxn) Get(key []byte) (val []byte, err error) {
	if key[0] == bucketPrefix {
		return nil, ErrInvalidKey
	}

	return t.get(key)
}

// Put will put a value for a given key
func (t *WTxn) Put(key []byte, val []byte) (err error) {
	if key[0] == bucketPrefix {
		return ErrInvalidKey
	}

	return t.put(key, val)
}

// Delete remove a value for a given key
func (t *WTxn) Delete(key []byte) (err error) {
	if key[0] == bucketPrefix {
		return ErrInvalidKey
	}

	return t.delete(key)
}
