package bench

import (
	"log"
	"os"
	"path"
	"testing"
	"time"

	"github.com/boltDB/bolt"
)

func openboltEnv(dbPath string) *bolt.DB {
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		log.Fatalln(err)
	}

	db, err := bolt.Open(path.Join(dbPath, "benchmark.bdb"), 0755, nil)
	if err != nil {
		log.Fatalln(err)
	}

	return db
}

func BenchmarkPutSmall_bolt(b *testing.B) {
	b.SetParallelism(1)

	take++
	t0 := time.Now()
	defer func() {
		log.Printf("take %d (n=%d) done in %v\n\n", take, b.N, time.Now().Sub(t0))
	}()

	log.Printf("bench put SMALL (8KB), take %d (n=%d)\n", take, b.N)
	benchPutbolt(b, 20*GB, SmallVal)
}

func BenchmarkPutLarge_bolt(b *testing.B) {
	b.SetParallelism(1)

	take++
	t0 := time.Now()
	defer func() {
		log.Printf("take %d (n=%d) done in %v\n\n", take, b.N, time.Now().Sub(t0))
	}()

	log.Printf("bench put LARGE (8MB), take %d (n=%d)", take, b.N)
	benchPutbolt(b, 20*GB, LargeVal)
}

func benchPutbolt(b *testing.B, size int64, val []byte) {
	defer rmDB(dbPath)

	db := openboltEnv(dbPath)
	defer db.Close()

	txn, err := db.Begin(true)
	checkErr(err)

	bucket, err := txn.CreateBucket(benchBucket)
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
		checkErr(bucket.Put(key, val))
		if i%batch == 0 {
			checkErr(txn.Commit())

			txn, err = db.Begin(true)
			checkErr(err)
			bucket = txn.Bucket(benchBucket)
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
