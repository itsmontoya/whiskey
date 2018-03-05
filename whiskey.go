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
	return NewWithSize(dir, name, InitialSize)
}

// NewWithSize will return a new DB with a requested minimum size
func NewWithSize(dir, name string, sz int64) (dbp *DB, err error) {
	var db DB
	if db.a, err = newallocator(dir, name, RW, sz); err != nil {
		return
	}

	db.wb = newbackend(db.a)
	if db.a.m.tail == metaSize {
		db.a.m.tail += pairSize
	} else {
		db.p = (*pair)(unsafe.Pointer(&db.a.mm[metaSize]))
		db.wb.p = *db.p
		db.wb.setBytes()
	}

	b := db.wb

	var tree *rbt.Tree
	if tree, err = rbt.NewRaw(InitialSize, func(sz int64) (bs []byte) {
		bs = b.Grow(sz)
		db.p = (*pair)(unsafe.Pointer(&db.a.mm[metaSize]))
		return
	}, b.Close); err != nil {
		return
	}

	db.txn = db.newTxn(tree)
	dbp = &db
	return
}

// DB represents a database
type DB struct {
	mux sync.RWMutex
	a   *allocator
	p   *pair
	wb  *backend

	txn *RTxn
}

func (db *DB) loadTxn() *RTxn {
	ptr := unsafe.Pointer(db.txn)
	return (*RTxn)(atomic.LoadPointer(&ptr))
}

func (db *DB) swapTxn(txn *RTxn) (old *RTxn) {
	ptr := unsafe.Pointer(&db.txn)
	uptr := (*unsafe.Pointer)(ptr)
	return (*RTxn)(atomic.SwapPointer(uptr, unsafe.Pointer(txn)))
}

func (db *DB) newTxn(t *rbt.Tree) *RTxn {
	var rtxn RTxn
	rtxn.p = *db.p
	rtxn.t = t
	rtxn.readers = 1
	return &rtxn
}

func (db *DB) initTxn(t *rbt.Tree) {
	rtxn := db.newTxn(t)
	old := db.swapTxn(rtxn)
	db.releaseReader(old)
	old.t.Close()
}

func (db *DB) releaseReader(txn *RTxn) {
	readers := txn.decReaders()
	if readers > 0 {
		return
	}

	db.a.release(txn.p.offset, txn.p.sz)
}

// Read will return a read transaction
func (db *DB) Read(fn TxnFn) (err error) {
	db.mux.RLock()
	txn := db.loadTxn()
	txn.incReaders()

	defer func() {
		db.releaseReader(txn)
		db.mux.RUnlock()
	}()

	err = fn(txn)
	return
}

// ReadTxn will return a read transaction
func (db *DB) ReadTxn(fn TxnFn) (txn Txn, close func()) {
	db.mux.RLock()

	rtxn := db.loadTxn()
	rtxn.incReaders()

	db.mux.RLock()
	close = func() {
		db.releaseReader(rtxn)
		db.mux.RUnlock()
	}

	txn = rtxn
	return
}

// Update will return an update transaction
func (db *DB) Update(fn TxnFn) (err error) {
	var txn WTxn
	db.mux.Lock()
	defer db.mux.Unlock()
	b := db.wb.dup()

	if txn.t, err = rbt.NewRaw(InitialSize, func(sz int64) (bs []byte) {
		bs = b.Grow(sz)
		db.p = (*pair)(unsafe.Pointer(&db.a.mm[metaSize]))
		return
	}, b.Close); err != nil {
		return
	}

	// This anon function might seem ridiculous, but for some reason not having the function caused
	// a performance regression, see below:
	// # Function removed
	// BenchmarkWhiskeyPut-16      1000      2116595 ns/op      392058 B/op      13001 allocs/op
	// # Function added
	// BenchmarkWhiskeyPut-16      1000      2073500 ns/op      376022 B/op      12000 allocs/op
	func() {
		if err = fn(&txn); err != nil {
			txn.t.Close()
			return
		}

		db.wb = b
		db.p.offset = b.p.offset
		db.p.sz = b.p.sz
		db.initTxn(txn.t)
	}()

	return
}

// UpdateTxn will return an update transaction
func (db *DB) UpdateTxn() (tp Txn, close func(commit bool), err error) {
	var txn WTxn
	db.mux.Lock()
	b := db.wb.dup()

	if txn.t, err = rbt.NewRaw(InitialSize, func(sz int64) (bs []byte) {
		bs = b.Grow(sz)
		db.p = (*pair)(unsafe.Pointer(&db.a.mm[metaSize]))
		return
	}, b.Close); err != nil {
		return
	}

	tp = &txn

	close = func(commit bool) {
		if commit {
			db.wb = b
			db.p.offset = b.p.offset
			db.p.sz = b.p.sz

			db.initTxn(txn.t)
		}

		db.mux.Unlock()
	}

	return
}

// Close will close an instance of DB
func (db *DB) Close() (err error) {
	var errs errors.ErrorList
	db.mux.Lock()
	defer db.mux.Unlock()
	errs.Push(db.a.Close())
	return errs.Err()
}
