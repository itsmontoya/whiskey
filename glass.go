package whiskey

import (
	"github.com/PathDNA/atoms"
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
	mux atoms.RWMux

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

	db.mux.Read(func() {
		err = fn(&txn)
	})

	txn.r = nil
	txn.kbuf = nil
	return
}

// Update will return an update transaction
func (db *DB) Update(fn TxnFn) (err error) {
	var txn Txn
	txn.r = db.w
	txn.w = db.s

	db.mux.Update(func() {
		defer db.s.Reset()
		if err = fn(&txn); err != nil {
			return
		}

		err = txn.flush()
	})

	txn.r = nil
	txn.w = nil
	txn.kbuf = nil
	return
}

// Close will close an instance of DB
func (db *DB) Close() (err error) {
	var errs errors.ErrorList
	db.mux.Update(func() {
		errs.Push(db.w.Close())
		errs.Push(db.s.Close())
	})

	return errs.Err()
}
