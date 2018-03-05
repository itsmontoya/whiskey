package whiskey

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/itsmontoya/rbt"
	"github.com/missionMeteora/toolkit/errors"
)

const (
	// ErrInvalidKey is returned when an invalid key is presented
	ErrInvalidKey = errors.Error("invalid key")
	// ErrKeyDoesNotExist is returned when a requested key does not exist
	ErrKeyDoesNotExist = errors.Error("key does not exist")
)

const (
	bucketPrefix = '_'
)

const (
	// InitialSize is the initial DB size
	InitialSize = 1024
)

// New will return a new DB
func New(dir, name string) (dbp *DB, err error) {
	var db DB
	if db.a, err = newallocator(dir, name, RW); err != nil {
		return
	}

	db.a.grow(InitialSize)

	db.wb = newbackend(db.a)
	if db.a.m.tail == metaSize {
		db.a.m.tail += pairSize
	} else {
		db.p = (*pair)(unsafe.Pointer(&db.a.mm[metaSize]))
		db.wb.p = *db.p
		db.wb.setBytes()
	}
	var tree *rbt.Tree
	if tree, err = rbt.NewRaw(InitialSize, func(sz int64) (bs []byte) {
		bs = db.wb.Grow(sz)
		db.p = (*pair)(unsafe.Pointer(&db.a.mm[metaSize]))
		return
	}, db.wb.Close); err != nil {
		return
	}

	db.tree = unsafe.Pointer(tree)
	dbp = &db
	return
}

// DB represents a database
type DB struct {
	mux sync.RWMutex
	a   *allocator
	p   *pair
	wb  *backend

	tree unsafe.Pointer

	rtxn Txn
	wtxn Txn
}

// Read will return a read transaction
func (db *DB) Read(fn TxnFn) (err error) {
	var txn RTxn
	txn.t = (*rbt.Tree)(atomic.LoadPointer(&db.tree))

	db.mux.RLock()
	defer db.mux.RUnlock()
	err = fn(&txn)
	return
}

// ReadTxn will return a read transaction
func (db *DB) ReadTxn(fn TxnFn) (tp Txn, close func()) {
	var txn RTxn
	txn.t = (*rbt.Tree)(atomic.LoadPointer(&db.tree))

	db.mux.RLock()
	close = db.mux.RUnlock
	tp = &txn
	return
}

// Update will return an update transaction
func (db *DB) Update(fn TxnFn) (err error) {
	var txn WTxn
	b := db.wb.dup()

	if txn.t, err = rbt.NewRaw(InitialSize, func(sz int64) (bs []byte) {
		bs = b.Grow(sz)
		db.p = (*pair)(unsafe.Pointer(&db.a.mm[metaSize]))
		return
	}, db.wb.Close); err != nil {
		return
	}

	db.mux.Lock()
	defer db.mux.Unlock()
	// This anon function might seem ridiculous, but for some reason not having the function caused
	// a performance regression, see below:
	// # Function removed
	// BenchmarkWhiskeyPut-16      1000      2116595 ns/op      392058 B/op      13001 allocs/op
	// # Function added
	// BenchmarkWhiskeyPut-16      1000      2073500 ns/op      376022 B/op      12000 allocs/op
	func() {
		if err = fn(&txn); err != nil {
			b.Close()
			return
		}

		atomic.StorePointer(&db.tree, unsafe.Pointer(txn.t))
		db.wb = b
		db.p.offset = b.p.offset
		db.p.sz = b.p.sz
	}()

	return
}

// UpdateTxn will return an update transaction
func (db *DB) UpdateTxn() (tp Txn, close func(), err error) {
	var txn WTxn
	ptr := unsafe.Pointer(db.wb)
	master := (*backend)(atomic.LoadPointer(&ptr))
	b := master.dup()

	if txn.t, err = rbt.NewRaw(InitialSize, func(sz int64) (bs []byte) {
		bs = b.Grow(sz)
		db.p = (*pair)(unsafe.Pointer(&db.a.mm[metaSize]))
		return
	}, db.wb.Close); err != nil {
		return
	}

	db.mux.Lock()
	tp = &txn

	close = func() {
		atomic.StorePointer(&db.tree, unsafe.Pointer(txn.t))
		db.wb = b
		db.p.offset = b.p.offset
		db.p.sz = b.p.sz
		db.mux.Unlock()
		return
	}

	return
}

// Close will close an instance of DB
func (db *DB) Close() (err error) {
	var errs errors.ErrorList
	db.mux.Lock()
	defer db.mux.Unlock()
	tree := (*rbt.Tree)(atomic.LoadPointer(&db.tree))
	errs.Push(tree.Close())

	errs.Push(db.wb.Close())
	errs.Push(db.a.Close())
	return errs.Err()
}
