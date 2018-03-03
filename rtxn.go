package whiskey

import (
	"github.com/itsmontoya/rbt"
	"github.com/missionMeteora/toolkit/errors"
)

const (
	// ErrCannotWrite is returned when a write action is attempted during a read transaction
	ErrCannotWrite = errors.Error("cannot write during a read transaction")
)

// RTxn is a transaction type
type RTxn struct {
	t *rbt.Tree

	kbuf []byte
	bkts []*Bucket
}

func (t *RTxn) setKeyBuffer(key []byte) {
	// Reset before using
	t.kbuf = t.kbuf[:0]
	// Append bucket prefix
	t.kbuf = append(t.kbuf, bucketPrefix)
	// Append key
	t.kbuf = append(t.kbuf, key...)
}

func (t *RTxn) grow(key []byte, sz int64) (bs []byte) {
	return t.t.Get(key)
}

func (t *RTxn) newBucket(key []byte) (bp *Bucket, err error) {
	var nt RTxn
	bp = newBucket(key, t.grow)
	if nt.t, err = rbt.NewRaw(bucketInitSize, bp.grow, nil); err != nil {
		return
	}

	bp.Txn = &nt
	return
}

func (t *RTxn) getBucket(key []byte) (*Bucket, error) {
	return t.newBucket(key)
}

// Bucket will return a bucket for a provided key
func (t *RTxn) Bucket(key []byte) (bp *Bucket, err error) {
	t.setKeyBuffer(key)
	bs := t.t.Get(t.kbuf)
	if bs == nil {
		// Bucket does not exist, bail out!
		err = ErrKeyDoesNotExist
		return
	}

	return t.getBucket(t.kbuf)
}

// CreateBucket will create a bucket for a provided key
func (t *RTxn) CreateBucket(key []byte) (bp *Bucket, err error) {
	return nil, ErrCannotWrite
}

// Get will retrieve a value for a given key
func (t *RTxn) Get(key []byte) (val []byte, err error) {
	if key[0] == bucketPrefix {
		return nil, ErrInvalidKey
	}

	val = t.t.Get(key)
	return
}

// Put will put a value for a given key
func (t *RTxn) Put(key []byte, val []byte) (err error) {
	return ErrCannotWrite
}

// Delete remove a value for a given key
func (t *RTxn) Delete(key []byte) (err error) {
	return ErrCannotWrite
}