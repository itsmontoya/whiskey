package whiskey

import (
	"bytes"

	"github.com/itsmontoya/rbt"
)

// WTxn is a transaction type
type WTxn struct {
	t *rbt.Tree

	kbuf []byte
	bkts []*Bucket
}

func (t *WTxn) setKeyBuffer(key []byte) {
	// Reset before using
	t.kbuf = t.kbuf[:0]
	// Append bucket prefix
	t.kbuf = append(t.kbuf, bucketPrefix)
	// Append key
	t.kbuf = append(t.kbuf, key...)
}

func (t *WTxn) newBucket(key []byte) (bp *Bucket, err error) {
	var nt WTxn
	bp = newBucket(key, t.grow)
	if nt.t, err = rbt.NewRaw(bucketInitSize, bp.grow, nil); err != nil {
		return
	}

	bp.Txn = &nt
	t.bkts = append(t.bkts, bp)
	return
}

func (t *WTxn) getBucket(key []byte) (bp *Bucket, err error) {
	for _, b := range t.bkts {
		if bytes.Equal(key, b.key) {
			bp = b
			return
		}
	}

	var bs []byte
	if bs = t.t.Get(key); bs == nil {
		return
	}

	return t.newBucket(key)
}

func (t *WTxn) grow(key []byte, sz int64) (bs []byte) {
	return t.t.Grow(key, sz)
}

// Bucket will return a bucket for a provided key
func (t *WTxn) Bucket(key []byte) (bp *Bucket, err error) {
	t.setKeyBuffer(key)
	return t.getBucket(t.kbuf)
}

// CreateBucket will create a bucket for a provided key
func (t *WTxn) CreateBucket(key []byte) (bp *Bucket, err error) {
	t.setKeyBuffer(key)
	if bp, err = t.getBucket(t.kbuf); err != nil {
		return
	}

	return t.newBucket(t.kbuf)
}

// Get will retrieve a value for a given key
func (t *WTxn) Get(key []byte) (val []byte, err error) {
	if key[0] == bucketPrefix {
		return nil, ErrInvalidKey
	}

	val = t.t.Get(key)
	return
}

// Put will put a value for a given key
func (t *WTxn) Put(key []byte, val []byte) (err error) {
	if key[0] == bucketPrefix {
		return ErrInvalidKey
	}

	t.t.Put(key, val)
	return
}

// Delete remove a value for a given key
func (t *WTxn) Delete(key []byte) (err error) {
	if key[0] == bucketPrefix {
		return ErrInvalidKey
	}

	t.t.Delete(key)
	return
}
