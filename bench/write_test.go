package bench

import (
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/itsmontoya/whiskey"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

var (
	BENCH_DB  = "bench.db"
	BENCH_DBI = []byte("bench_dbi")
)

func rmDB(dbPath string) error {
	return os.Remove(dbPath)
}

func openEnv(dbPath string) *whiskey.DB {
	db, err := whiskey.New("", dbPath)
	if err != nil {
		log.Fatalln(err)
	}
	return db
}

var (
	SMALL_VAL = []byte(strings.Repeat("ABCD", 2*KB)) // 8KB
	LARGE_VAL = []byte(strings.Repeat("ABCD", 2*MB)) // 8MB
)

const (
	KB = 1024
	MB = 1024 * 1024
	GB = 1024 * 1024 * 1024
)

var take int

func BenchmarkPutSmall_Whiskey(b *testing.B) {
	b.SetParallelism(1)

	take++
	t0 := time.Now()
	defer func() {
		log.Printf("take %d (n=%d) done in %v\n\n", take, b.N, time.Now().Sub(t0))
	}()

	log.Printf("bench put SMALL (8KB), take %d (n=%d)\n", take, b.N)
	benchPut_Whiskey(b, 20*GB, SMALL_VAL)
}

func BenchmarkPutLarge_Whiskey(b *testing.B) {
	b.SetParallelism(1)

	take++
	t0 := time.Now()
	defer func() {
		log.Printf("take %d (n=%d) done in %v\n\n", take, b.N, time.Now().Sub(t0))
	}()

	log.Printf("bench put LARGE (8MB), take %d (n=%d)", take, b.N)
	benchPut_Whiskey(b, 20*GB, LARGE_VAL)
}

func benchPut_Whiskey(b *testing.B, size int64, val []byte) {
	db := openEnv(BENCH_DB)
	// db := openEnv(fmt.Sprintf("W%d", len(val)/1024) + BENCH_DB)
	defer rmDB(BENCH_DB)
	defer db.Close()

	txn, txnClose := db.UpdateTxn()
	bucket, err := txn.CreateBucket(BENCH_DBI)
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
			txnClose()
			txn, txnClose = db.UpdateTxn()
			bucket = txn.Bucket(BENCH_DBI)
		}
		if i == b.N-1 {
			checkErr(txn.Commit())
			txnClose()
			// print the duration of the last run
			log.Println("last put took:", time.Now().Sub(t0))
			log.Println("last key:", string(key))
		}
	}
	list(db)
	mem()
}

func checkErr(err error) {
	if err != nil {
		log.Fatalln("[ERR]", err)
	}
}

var clock int

var src = rand.New(rand.NewSource(1))

// sortedKey returns key of (9 bytes of clock + 7 (or 14 chars) random bytes).
func sortedKey() []byte {
	clock++
	randPart := make([]byte, 7)
	src.Read(randPart)
	key := fmt.Sprintf("%09d", clock) + hex.EncodeToString(randPart)
	return []byte(key)
}

func randKey() []byte {
	randPart := make([]byte, 16)
	src.Read(randPart)
	return []byte(hex.EncodeToString(randPart))
}

func decKey() []byte {
	clock++
	return []byte(fmt.Sprintf("%09d", clock))
}

func useKey(n int) []byte {
	return []byte(fmt.Sprintf("%09d", n))
}

func list(db *whiskey.DB) {
	// txn, txnClose := db.ReadTxn(nil)
	// defer txnClose()

	// stats := txn.Bucket(BENCH_DBI).
	// checkErr(err)
	// log.Printf("depth: %d, branch pg: %d, leaf pg: %d, entries: %d\n",
	// 	stats.Depth, stats.BranchPageN, stats.LeafPageN, stats.KeyN)
}

func mem() {
	t0 := time.Now()
	runtime.GC()
	log.Println("gc:", time.Now().Sub(t0))
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	log.Printf("mem: heap %dMB/%dMB, alloc total %dMB\n", stats.HeapInuse/MB, stats.HeapAlloc/MB, stats.TotalAlloc/MB)
}
