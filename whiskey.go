package whiskey

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/PathDNA/atoms"

	"github.com/itsmontoya/rbt"
	"github.com/itsmontoya/rbt/allocator"
	"github.com/itsmontoya/rbt/backend"
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
	if db.a, err = allocator.NewMMap(dir, name); err != nil {
		return
	}

	db.a.Grow(sz)
	db.mb = backend.NewMulti(db.a)
	db.be = db.mb.Get()

	db.a.OnGrow(db.onGrow)

	var t *rbt.Tree
	if t, err = rbt.NewRaw(InitialSize, db.be, db.a); err != nil {
		return
	}

	db.txn = db.newTxn(t)
	dbp = &db
	return
}

// DB represents a database
type DB struct {
	mux sync.RWMutex
	a   allocator.Allocator
	mb  *backend.Multi
	be  *backend.Backend

	txn *RTxn

	closed atoms.Bool
}

func (db *DB) onGrow() (end bool) {
	if db.closed.Get() {
		return true
	}

	db.mb.Set(db.be)
	return
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
	rtxn.t = t
	rtxn.readers = 1
	return &rtxn
}

func (db *DB) initTxn(t *rbt.Tree, b *backend.Backend) {
	rtxn := db.newTxn(t)
	old := db.swapTxn(rtxn)
	db.be = b
	db.releaseReader(old)
}

func (db *DB) releaseReader(txn *RTxn) {
	readers := txn.decReaders()
	if readers > 0 {
		return
	}

	txn.t.Destroy()
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
	b := db.be.Dup()

	if txn.t, err = rbt.NewRaw(InitialSize, b, db.a); err != nil {
		return
	}

	txn.t.Checkout()

	// This anon function might seem ridiculous, but for some reason not having the function caused
	// a performance regression, see below:
	// # Function removed
	// BenchmarkWhiskeyPut-16      1000      2116595 ns/op      392058 B/op      13001 allocs/op
	// # Function added
	// BenchmarkWhiskeyPut-16      1000      2073500 ns/op      376022 B/op      12000 allocs/op
	func() {
		if err = fn(&txn); err != nil {
			txn.t.Destroy()
			return
		}

		b.Notify()
		db.initTxn(txn.t, b)
	}()

	return
}

// UpdateTxn will return an update transaction
func (db *DB) UpdateTxn() (tp Txn, close func(commit bool), err error) {
	var txn WTxn
	db.mux.Lock()
	b := db.be.Dup()

	if txn.t, err = rbt.NewRaw(InitialSize, b, db.a); err != nil {
		return
	}

	txn.t.Checkout()

	tp = &txn

	close = func(commit bool) {
		if commit {
			b.Notify()
			db.initTxn(txn.t, b)
		} else {
			txn.t.Destroy()
		}

		db.mux.Unlock()
	}

	return
}

// Close will close an instance of DB
func (db *DB) Close() (err error) {
	if !db.closed.Set(true) {
		return errors.ErrIsClosed
	}

	var errs errors.ErrorList
	db.mux.Lock()
	defer db.mux.Unlock()
	errs.Push(db.a.Close())
	return errs.Err()
}
