package bench

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/zenhotels/lmdb-go/lmdb"
)

func openLMDBEnv(dbPath string) lmdb.Env {
	var err error
	if err = os.MkdirAll(dbPath, 0755); err != nil {
		log.Fatalln(err)
	}

	var env lmdb.Env
	if env, err = lmdb.EnvCreate(); err != nil {
		log.Fatalln(err)
	}

	if err = env.SetMaxDBs(1); err != nil {
		log.Fatalln(err)
	}

	if err = env.Open(dbPath, 0, 0644); err != nil {
		log.Fatalln(err)
	}

	return env
}

func BenchmarkPutSmall_LMDB(b *testing.B) {
	b.SetParallelism(1)

	take++
	t0 := time.Now()
	defer func() {
		log.Printf("take %d (n=%d) done in %v\n\n", take, b.N, time.Now().Sub(t0))
	}()

	log.Printf("bench put SMALL (8KB), take %d (n=%d)\n", take, b.N)
	benchPutLMDB(b, 20*GB, SmallVal)
}

func BenchmarkPutLarge_LMDB(b *testing.B) {
	b.SetParallelism(1)

	take++
	t0 := time.Now()
	defer func() {
		log.Printf("take %d (n=%d) done in %v\n\n", take, b.N, time.Now().Sub(t0))
	}()

	log.Printf("bench put LARGE (8MB), take %d (n=%d)", take, b.N)
	benchPutLMDB(b, 20*GB, LargeVal)
}

func benchPutLMDB(b *testing.B, size int64, val []byte) {
	defer rmDB(dbPath)
	env := openLMDBEnv(dbPath)
	defer env.Close()

	checkErr(env.SetMapSize(uint(size)))
	txn, err := env.BeginTxn(nil, lmdb.DefaultTxnFlags)
	checkErr(err)
	bucket, err := txn.DbiOpen(string(benchBucket), lmdb.DbiCreate)
	checkErr(err)

	var t0 time.Time

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(val)))

	batch := 400

	for i := 0; i < b.N; i++ {
		if i == b.N-1 {
			// record time of the last run
			t0 = time.Now()
		}

		// key := sortedKey()
		key := randKey()
		checkErr(txn.Put(bucket, key, val, 0))
		if i%batch == 0 {
			checkErr(txn.Commit())
			txn, err := env.BeginTxn(nil, lmdb.DefaultTxnFlags)
			checkErr(err)
			bucket, err = txn.DbiOpen(string(benchBucket), lmdb.DefaultDbiFlags)
			checkErr(err)

		}
		if i == b.N-1 {
			checkErr(txn.Commit())
			// print the duration of the last run
			log.Println("last put took:", time.Now().Sub(t0))
			log.Println("last key:", string(key))
		}
	}

	mem()
}
