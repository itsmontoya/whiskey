package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"time"
)

var (
	dbPath      = "data"
	benchBucket = []byte("bench_bkt")
)

// Value block
var (
	SmallVal = []byte(strings.Repeat("ABCD", 2*KB)) // 8KB
	LargeVal = []byte(strings.Repeat("ABCD", 2*MB)) // 8MB
)

// Sizing reference block
const (
	KB = 1024
	MB = 1024 * 1024
	GB = 1024 * 1024 * 1024
)

var take int

var clock int

var src = rand.New(rand.NewSource(1))

func main() {
	//	batchSize := int64(10000)
	//	bench(100, 100, 3)
	//bench(500, 100, 3)
	bench(5000, 500, 3)

	os.Exit(0)
}

func bench(benchCount, batchSize, iterations int64) {
	benchFunc(whiskeyPut, benchCount, batchSize, iterations)
	benchFunc(boltPut, benchCount, batchSize, iterations)
	benchFunc(lmdbPut, benchCount, batchSize, iterations)
}

func benchFunc(fn benchFn, benchCount, batchSize, iterations int64) {
	for i := int64(0); i < iterations; i++ {
		fn(benchCount, batchSize, 2*GB, SmallVal)
	}
}

func rmDB(dbPath string) error {
	return os.RemoveAll(dbPath)
}

func checkErr(err error) {
	if err != nil {
		log.Fatalln("[ERR]", err)
	}
}

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

func mem() {
	t0 := time.Now()
	runtime.GC()
	log.Println("gc:", time.Now().Sub(t0))
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	log.Printf("mem: heap %dMB/%dMB, alloc total %dMB\n", stats.HeapInuse/MB, stats.HeapAlloc/MB, stats.TotalAlloc/MB)
}

type benchFn = func(benchCount, batchSize, envSize int64, val []byte)
