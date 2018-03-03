package whiskey

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/boltDB/bolt"

	"github.com/itsmontoya/rbt/testUtils"
	"github.com/missionMeteora/toolkit/errors"
)

var (
	testSortedList  = testUtils.GetSorted(1000)
	testReverseList = testUtils.GetReverse(1000)
	testRandomList  = testUtils.GetRand(1000)

	testSortedListStr  = testUtils.GetStrSlice(testSortedList)
	testReverseListStr = testUtils.GetStrSlice(testReverseList)
	testRandomListStr  = testUtils.GetStrSlice(testRandomList)

	testVal     []byte
	testBktName = []byte("testbkt")
)

func TestWhiskey(t *testing.T) {
	var (
		db  *DB
		err error
	)

	if err = os.MkdirAll("./testing", 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./testing")

	if db, err = New("./testing", "data.db"); err != nil {
		t.Fatal(err)
	}
	//defer db.Close()

	if err = db.Update(func(txn Txn) (err error) {
		var bkt *Bucket
		if bkt, err = txn.CreateBucket([]byte("basic")); err != nil {
			return
		}

		bkt.Put([]byte("name"), []byte("Josh"))
		bkt.Put([]byte("1"), []byte("1"))
		bkt.Put([]byte("2"), []byte("2"))
		bkt.Put([]byte("3"), []byte("3"))
		bkt.Put([]byte("4"), []byte("4"))
		bkt.Put([]byte("5"), []byte("5"))
		bkt.Put([]byte("6"), []byte("6"))
		bkt.Put([]byte("7"), []byte("7"))
		bkt.Put([]byte("8"), []byte("8"))
		bkt.Put([]byte("9"), []byte("9"))

		var val []byte
		if val, err = bkt.Get([]byte("name")); err != nil {
			return
		}

		if string(val) != "Josh" {
			return fmt.Errorf("invalid value, expected Josh and received %s", string(val))
		}

		return
	}); err != nil {
		t.Fatal(err)
	}

	if err = db.Read(func(txn Txn) (err error) {
		var bkt *Bucket
		if bkt, err = txn.Bucket([]byte("basic")); err != nil {
			return
		}

		if err = bkt.Put([]byte("name"), []byte("Josh")); err == nil {
			return errors.Error("expected error, received nil")
		}

		var val []byte
		if val, err = bkt.Get([]byte("name")); err != nil {
			return
		}

		if string(val) != "Josh" {
			return fmt.Errorf("invalid value, expected Josh and received %s", string(val))
		}

		return
	}); err != nil {
		t.Fatal(err)
	}

	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	if db, err = New("./testing", "data.db"); err != nil {
		t.Fatal(err)
	}

	if err = db.Read(func(txn Txn) (err error) {
		var bkt *Bucket
		if bkt, err = txn.Bucket([]byte("basic")); err != nil {
			return
		}

		var val []byte
		if val, err = bkt.Get([]byte("name")); err != nil {
			return
		}

		if string(val) != "Josh" {
			return fmt.Errorf("invalid value, expected Josh and received %s", string(val))
		}

		return
	}); err != nil {
		t.Fatal(err)
	}

}

func TestPut(t *testing.T) {
	var (
		db  *DB
		err error
	)

	if err = os.MkdirAll("./testing", 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./testing")

	if db, err = New("./testing", "test_put"); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, kv := range testSortedListStr {
		if err = db.Update(func(txn Txn) (err error) {
			var bkt *Bucket
			if bkt, err = txn.CreateBucket(testBktName); err != nil {
				return
			}

			if err = bkt.Put(kv.Val, kv.Val); err != nil {
				return
			}

			var val []byte
			if val, err = bkt.Get(kv.Val); err != nil {
				return
			}

			if !bytes.Equal(kv.Val, val) {
				t.Fatalf("invalid value, expected \"%s\" and received \"%s\"", string(kv.Val), string(val))
			}

			return
		}); err != nil {
			t.Fatal(err)
		}
	}

}

func BenchmarkWhiskeyGet(b *testing.B) {
	var (
		db  *DB
		err error
	)

	if err = os.MkdirAll("testing", 0755); err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("testing")

	if db, err = New("testing", "benchmarks"); err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	for _, kv := range testSortedListStr {

		if err = db.Update(func(txn Txn) (err error) {
			var bkt *Bucket
			if bkt, err = txn.CreateBucket(testBktName); err != nil {
				return
			}

			if err = bkt.Put(kv.Val, kv.Val); err != nil {
				return
			}

			return
		}); err != nil {
			b.Fatal(err)
		}

	}

	//	return
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, kv := range testSortedListStr {
			db.Read(func(txn Txn) (err error) {
				var bkt *Bucket
				if bkt, err = txn.Bucket(testBktName); err != nil {
					return
				}

				testVal, err = bkt.Get(kv.Val)
				return
			})
		}
	}

	b.ReportAllocs()
}

func BenchmarkWhiskeyPut(b *testing.B) {
	var (
		db  *DB
		err error
	)

	if err = os.MkdirAll("testing", 0755); err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("testing")

	if db, err = New("testing", "benchmarks"); err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, kv := range testSortedListStr {
			if err = db.Update(func(txn Txn) (err error) {
				var bkt *Bucket
				if bkt, err = txn.CreateBucket(testBktName); err != nil {
					return
				}

				return bkt.Put(kv.Val, kv.Val)
			}); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ReportAllocs()
}

func BenchmarkWhiskeyBatchPut(b *testing.B) {
	var (
		db  *DB
		err error
	)

	if err = os.MkdirAll("testing", 0755); err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("testing")

	if db, err = New("testing", "benchmarks"); err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err = db.Update(func(txn Txn) (err error) {
			var bkt *Bucket
			if bkt, err = txn.CreateBucket(testBktName); err != nil {
				return
			}

			for _, kv := range testSortedListStr {
				if err = bkt.Put(kv.Val, kv.Val); err != nil {
					return
				}
			}

			return
		}); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
}

func BenchmarkBoltGet(b *testing.B) {
	var (
		db  *bolt.DB
		err error
	)

	if err = os.MkdirAll("testing", 0755); err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("testing")

	if db, err = bolt.Open("testing/benchmarks.bdb", 0644, nil); err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if err = db.Update(func(txn *bolt.Tx) (err error) {
		var bkt *bolt.Bucket
		if bkt, err = txn.CreateBucket(testBktName); err != nil && err != bolt.ErrBucketExists {
			return
		}

		for _, kv := range testSortedListStr {
			if err = bkt.Put(kv.Val, kv.Val); err != nil {
				return
			}
		}

		return
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, kv := range testSortedListStr {
			db.View(func(txn *bolt.Tx) (err error) {
				bkt := txn.Bucket(testBktName)
				testVal = bkt.Get(kv.Val)
				return
			})
		}
	}

	b.ReportAllocs()
}

func BenchmarkBoltPut(b *testing.B) {
	var (
		db  *bolt.DB
		err error
	)

	if err = os.MkdirAll("testing", 0755); err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("testing")

	if db, err = bolt.Open("testing/benchmarks.bdb", 0644, nil); err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if err = db.Update(func(txn *bolt.Tx) (err error) {
		_, err = txn.CreateBucket(testBktName)
		return
	}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, kv := range testSortedListStr {
			if err = db.Update(func(txn *bolt.Tx) (err error) {
				bkt := txn.Bucket(testBktName)
				return bkt.Put(kv.Val, kv.Val)
			}); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ReportAllocs()
}

func BenchmarkBoltBatchPut(b *testing.B) {
	var (
		db  *bolt.DB
		err error
	)

	if err = os.MkdirAll("testing", 0755); err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("testing")

	if db, err = bolt.Open("testing/benchmarks.bdb", 0644, nil); err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if err = db.Update(func(txn *bolt.Tx) (err error) {
		_, err = txn.CreateBucket(testBktName)
		return
	}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err = db.Update(func(txn *bolt.Tx) (err error) {
			bkt := txn.Bucket(testBktName)

			for _, kv := range testSortedListStr {
				if err = bkt.Put(kv.Val, kv.Val); err != nil {
					return
				}
			}

			return
		}); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
}

func BenchmarkLMDBGet(b *testing.B) {
	var (
		env *lmdb.Env
		err error

		testBktNameStr = string(testBktName)
	)

	if err = os.MkdirAll("testing", 0755); err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("testing")

	if env, err = lmdb.NewEnv(); err != nil {
		b.Fatal(err)
	}

	if err = env.SetMaxDBs(3); err != nil {
		b.Fatal(err)
	}

	if err = env.Open("testing", 0, 0644); err != nil {
		b.Fatal(err)
	}
	defer env.Close()

	if err = env.Update(func(txn *lmdb.Txn) (err error) {
		var bkt lmdb.DBI
		if bkt, err = txn.CreateDBI(testBktNameStr); err != nil {
			return
		}

		for _, kv := range testSortedListStr {
			if err = txn.Put(bkt, kv.Val, kv.Val, 0); err != nil {
				return
			}
		}

		return
	}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, kv := range testSortedListStr {
			if err = env.Update(func(txn *lmdb.Txn) (err error) {
				var bkt lmdb.DBI
				if bkt, err = txn.OpenDBI(testBktNameStr, 0); err != nil {
					return
				}

				if testVal, err = txn.Get(bkt, kv.Val); err != nil {
					return
				}

				return
			}); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ReportAllocs()
}

func BenchmarkLMDBPut(b *testing.B) {
	var (
		env *lmdb.Env
		err error

		testBktNameStr = string(testBktName)
	)

	if err = os.MkdirAll("testing", 0755); err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("testing")

	if env, err = lmdb.NewEnv(); err != nil {
		b.Fatal(err)
	}

	if err = env.SetMaxDBs(3); err != nil {
		b.Fatal(err)
	}

	if err = env.Open("testing", 0, 0644); err != nil {
		b.Fatal(err)
	}
	defer env.Close()

	if err = env.Update(func(txn *lmdb.Txn) (err error) {
		_, err = txn.CreateDBI(testBktNameStr)
		return
	}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, kv := range testSortedListStr {
			if err = env.Update(func(txn *lmdb.Txn) (err error) {
				var bkt lmdb.DBI
				if bkt, err = txn.OpenDBI(testBktNameStr, 0); err != nil {
					return
				}

				return txn.Put(bkt, kv.Val, kv.Val, 0)
			}); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ReportAllocs()
}

func BenchmarkLMDBBatchPut(b *testing.B) {
	var (
		env *lmdb.Env
		err error

		testBktNameStr = string(testBktName)
	)

	if err = os.MkdirAll("testing", 0755); err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("testing")

	if env, err = lmdb.NewEnv(); err != nil {
		b.Fatal(err)
	}

	if err = env.SetMaxDBs(3); err != nil {
		b.Fatal(err)
	}

	if err = env.Open("testing", 0, 0644); err != nil {
		b.Fatal(err)
	}
	defer env.Close()

	if err = env.Update(func(txn *lmdb.Txn) (err error) {
		_, err = txn.CreateDBI(testBktNameStr)
		return
	}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err = env.Update(func(txn *lmdb.Txn) (err error) {
			var bkt lmdb.DBI
			if bkt, err = txn.OpenDBI(testBktNameStr, 0); err != nil {
				return
			}

			for _, kv := range testSortedListStr {
				if err = txn.Put(bkt, kv.Val, kv.Val, 0); err != nil {
					return
				}
			}

			return
		}); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
}
