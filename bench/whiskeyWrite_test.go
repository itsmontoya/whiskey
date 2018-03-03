package bench

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/itsmontoya/whiskey"
)

func openWhiskeyEnv(dbPath string) *whiskey.DB {
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		log.Fatalln(err)
	}

	db, err := whiskey.New(dbPath, "benchmark")
	if err != nil {
		log.Fatalln(err)
	}

	return db
}

func BenchmarkPutSmall_Whiskey(b *testing.B) {
	b.SetParallelism(1)

	take++
	t0 := time.Now()
	defer func() {
		log.Printf("take %d (n=%d) done in %v\n\n", take, b.N, time.Now().Sub(t0))
	}()

	log.Printf("bench put SMALL (8KB), take %d (n=%d)\n", take, b.N)
	benchPutWhiskey(b, 20*GB, SmallVal)
}

func BenchmarkPutLarge_Whiskey(b *testing.B) {
	b.SetParallelism(1)

	take++
	t0 := time.Now()
	defer func() {
		log.Printf("take %d (n=%d) done in %v\n\n", take, b.N, time.Now().Sub(t0))
	}()

	log.Printf("bench put LARGE (8MB), take %d (n=%d)", take, b.N)
	benchPutWhiskey(b, 20*GB, LargeVal)
}

func benchPutWhiskey(b *testing.B, size int64, val []byte) {
	defer rmDB(dbPath)
	db := openWhiskeyEnv(dbPath)
	defer db.Close()

	txn, txnClose, err := db.UpdateTxn()
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
			//	checkErr(txn.Commit())
			txnClose()
			txn, txnClose, err = db.UpdateTxn()
			checkErr(err)
			bucket, err = txn.Bucket(benchBucket)
			checkErr(err)
		}
		if i == b.N-1 {
			//			checkErr(txn.Commit())
			txnClose()
			// print the duration of the last run
			log.Println("last put took:", time.Now().Sub(t0))
			log.Println("last key:", string(key))
		}
	}

	mem()
}
