package whiskey

import (
	"sync"

	"github.com/itsmontoya/rbt"
	"github.com/missionMeteora/toolkit/errors"
)

const (
	// ErrInvalidKey is returned when an invalid key is presented
	ErrInvalidKey = errors.Error("invalid key")
)

const (
	bucketPrefix = '_'
)

// New will return a new DB
func New(dir, name string) (dbp *DB, err error) {
	var db DB
	if db.w, err = rbt.NewMMAP(dir, name+".wdb", 1024); err != nil {
		return
	}

	if db.s, err = rbt.NewMMAP(dir, name+".scratch.wdb", 1024); err != nil {
		return
	}

	dbp = &db
	return
}

// DB represents a database
type DB struct {
	mux sync.RWMutex

	w *rbt.Tree
	// Scratch disk
	s *rbt.Tree

	rtxn *Txn
	wtxn *Txn
}

// Read will return a read transaction
func (db *DB) Read(fn TxnFn) (err error) {
	var txn Txn
	txn.r = db.w

	db.mux.RLock()
	defer db.mux.RUnlock()
	err = fn(&txn)
	return
}

// ReadTxn will return a read transaction
func (db *DB) ReadTxn(fn TxnFn) (tp *Txn, close func()) {
	var txn Txn
	txn.r = db.w

	db.mux.RLock()
	close = db.mux.RUnlock
	tp = &txn
	return
}

// Update will return an update transaction
func (db *DB) Update(fn TxnFn) (err error) {
	var txn Txn
	txn.r = db.w
	txn.w = db.s

	db.mux.Lock()
	defer db.mux.Unlock()
	defer db.s.Reset()

	if err = fn(&txn); err != nil {
		return
	}

	err = txn.flush()

	txn.r = nil
	txn.w = nil
	txn.kbuf = nil
	return
}

// UpdateTxn will return an update transaction
func (db *DB) UpdateTxn() (tp *Txn, close func()) {
	var txn Txn
	txn.r = db.w
	txn.w = db.s

	db.mux.Lock()

	tp = &txn

	close = func() {
		txn.r = nil
		txn.w = nil
		txn.kbuf = nil

		db.s.Reset()
		db.mux.RUnlock()
	}

	return
}

// Close will close an instance of DB
func (db *DB) Close() (err error) {
	var errs errors.ErrorList
	db.mux.Lock()
	defer db.mux.Unlock()
	errs.Push(db.w.Close())
	errs.Push(db.s.Close())
	return errs.Err()
}
