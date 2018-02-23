package bench

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/itsmontoya/whiskey"
)

func BenchmarkGetSmall_Whiskey(b *testing.B) {
	b.SetParallelism(1)

	take++
	t0 := time.Now()
	defer func() {
		log.Printf("take %d (n=%d) done in %v\n\n", take, b.N, time.Now().Sub(t0))
	}()

	log.Printf("bench get SMALL (8KB), take %d (n=%d)\n", take, b.N)
	benchGet_Whiskey(b, 20*GB, SMALL_VAL)
}

func BenchmarkGetLarge_Whiskey(b *testing.B) {
	b.SetParallelism(1)

	take++
	t0 := time.Now()
	defer func() {
		log.Printf("take %d (n=%d) done in %v\n\n", take, b.N, time.Now().Sub(t0))
	}()

	log.Printf("bench get LARGE (8MB), take %d (n=%d)", take, b.N)
	benchGet_Whiskey(b, 20*GB, LARGE_VAL)
}

func benchGet_Whiskey(b *testing.B, size uint, val []byte) {
	db, err := whiskey.New("", fmt.Sprintf("W%d", len(val)/1024)+BENCH_DB)
	checkErr(err)
	defer db.Close()

	txn, txnClose := db.ReadTxn(nil)
	bucket := txn.Bucket(BENCH_DBI)

	b.SetBytes(int64(len(val)))
	b.ResetTimer()
	b.ReportAllocs()

	var found int
	var missed int
	for i := 0; i < b.N; i++ {
		// key := useKey(src.Intn(count + count/20))
		key := randKey()
		v, err := bucket.Get(key)
		checkErr(err)
		if v == nil {
			missed++
		} else {
			found++
			if len(v) != len(val) {
				log.Fatalln("expected", len(val), "but got", len(v))
			}
		}
	}
	txnClose()
	log.Println("missed:", missed, "found:", found)
	list(db)
	mem()
}
