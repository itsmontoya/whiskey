package whiskey

import (
	"bytes"

	"github.com/itsmontoya/rbt"
	"github.com/missionMeteora/toolkit/errors"
)

const (
	// ErrCannotWrite is returned when a write action is attempted during a read transaction
	ErrCannotWrite = errors.Error("cannot write during a read transaction")
)

// Txn is a transaction type
type Txn struct {
	r *rbt.Tree
	w *rbt.Tree

	kbuf []byte
	bkts []*Bucket
}

func (t *Txn) setKeyBuffer(key []byte) {
	// Reset before using
	t.kbuf = t.kbuf[:0]
	// Append bucket prefix
	t.kbuf = append(t.kbuf, bucketPrefix)
	// Append key
	t.kbuf = append(t.kbuf, key...)
}

func (t *Txn) getBucketBytes(key []byte) (rbs, wbs []byte) {
	t.setKeyBuffer(key)
	if t.r != nil {
		rbs = t.r.Get(t.kbuf)
	}

	if t.w != nil {
		// This is a write transaction, let's check if this value has been changed
		wbs = t.w.Get(t.kbuf)
	}

	return
}

func (t *Txn) getRoot(key []byte, sz int64) (bs []byte) {
	return t.r.Get(key)
}

func (t *Txn) getBucket(key []byte) *Bucket {
	for _, b := range t.bkts {
		if bytes.Equal(key, b.key) {
			return b
		}
	}

	return nil
}

func (t *Txn) truncateScratch(key []byte, sz int64) (bs []byte) {
	//	journaler.Debug("Growing scratch: %d", sz)
	return t.w.Grow(key, sz)
}

func (t *Txn) truncateRoot(key []byte, sz int64) (bs []byte) {
	//	journaler.Debug("Growing root: %d", sz)
	return t.r.Grow(key, sz)
}

// Bucket will return a bucket for a provided key
func (t *Txn) Bucket(key []byte) (bp *Bucket) {
	t.setKeyBuffer(key)
	if t.w != nil {
		if bp = t.getBucket(t.kbuf); bp != nil {
			return
		}
	}

	var rgfn, sgfn GrowFn
	if t.r != nil {
		rgfn = t.getRoot
	}

	if t.w != nil {
		sgfn = t.truncateScratch
	}

	bp = newBucket(t.kbuf, rgfn, sgfn)
	if t.w != nil {
		t.bkts = append(t.bkts, bp)
	}

	return
}

// CreateBucket will create a bucket for a provided key
func (t *Txn) CreateBucket(key []byte) (bp *Bucket, err error) {
	if t.w == nil {
		err = ErrCannotWrite
		return
	}

	t.setKeyBuffer(key)
	if bp = t.getBucket(t.kbuf); bp != nil {
		return
	}

	var rgfn, sgfn GrowFn
	if t.r != nil {
		rgfn = t.getRoot
	}

	if t.w != nil {
		sgfn = t.truncateScratch
	}

	bp = newBucket(t.kbuf, rgfn, sgfn)
	t.bkts = append(t.bkts, bp)
	return
}

// Get will retrieve a value for a given key
func (t *Txn) Get(key []byte) (val []byte, err error) {
	if key[0] == bucketPrefix {
		return nil, ErrInvalidKey
	}

	if t.w != nil {
		if val = t.w.Get(key); val != nil {
			return
		}
	}

	if t.r != nil {
		val = t.r.Get(key)
	}

	return
}

// Put will put a value for a given key
func (t *Txn) Put(key []byte, val []byte) (err error) {
	if key[0] == bucketPrefix {
		return ErrInvalidKey
	}

	if t.w == nil {
		return ErrCannotWrite
	}

	t.w.Put(key, val)
	return
}

func (t *Txn) writeEntry(key, val []byte) (end bool) {
	if key[0] == bucketPrefix {
		return
	}

	// Flush value to main branch
	t.r.Put(key, val)
	return
}

func (t *Txn) flush() (err error) {
	for _, b := range t.bkts {
		b.rgfn = t.truncateRoot
		if b.r == nil {
			if b.r, err = rbt.NewRaw(bucketInitSize, b.growRoot, nil); err != nil {
				return
			}
		}

		if err = b.flush(); err != nil {
			return
		}
	}

	t.w.ForEach(t.writeEntry)
	return
}

// TxnFn is a transaction func
type TxnFn func(txn *Txn) error
