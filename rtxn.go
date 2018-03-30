package whiskey

import (
	"sync/atomic"

	"github.com/itsmontoya/rbt"
	"github.com/itsmontoya/rbt/allocator"
	"github.com/missionMeteora/toolkit/errors"
)

const (
	// ErrCannotWrite is returned when a write action is attempted during a read transaction
	ErrCannotWrite = errors.Error("cannot write during a read transaction")
)

// RTxn is a transaction type
type RTxn struct {
	t *rbt.Tree
	s *allocator.Section

	readers int64
}

func (t *RTxn) incReaders() (new int64) {
	return atomic.AddInt64(&t.readers, 1)
}

func (t *RTxn) decReaders() (new int64) {
	return atomic.AddInt64(&t.readers, -1)
}

func (t *RTxn) newBucket(key []byte) (bp *Bucket) {
	return newBucket(key, t)
}

func (t *RTxn) bucket(key []byte) (bp *Bucket, err error) {
	var bs []byte
	if bs = t.t.Get(key); bs == nil {
		err = ErrKeyDoesNotExist
		return
	}

	bp = t.newBucket(key)
	return
}

func (t *RTxn) createBucket(key []byte) (bp *Bucket, err error) {
	return nil, ErrCannotWrite
}

func (t *RTxn) get(key []byte) (val []byte, err error) {
	val = t.t.Get(key)
	return
}

func (t *RTxn) put(key, val []byte) (err error) {
	return ErrCannotWrite
}

func (t *RTxn) delete(key []byte) (err error) {
	return ErrCannotWrite
}

// Bucket will return a bucket for a provided key
func (t *RTxn) Bucket(key []byte) (bp *Bucket, err error) {
	return t.bucket(getBucketKey(key))
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

	return t.get(key)
}

// Put will put a value for a given key
func (t *RTxn) Put(key []byte, val []byte) (err error) {
	return ErrCannotWrite
}

// Delete remove a value for a given key
func (t *RTxn) Delete(key []byte) (err error) {
	return ErrCannotWrite
}
