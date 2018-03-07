package main

import (
	"log"
	"testing"
	"time"
)

func BenchmarkGetSmall_Bolt(b *testing.B) {
	b.SetParallelism(1)

	take++
	t0 := time.Now()
	defer func() {
		log.Printf("take %d (n=%d) done in %v\n\n", take, b.N, time.Now().Sub(t0))
	}()

	log.Printf("bench get SMALL (8KB), take %d (n=%d)\n", take, b.N)
	benchGetBolt(b, 20*GB, SmallVal)
}

func BenchmarkGetLarge_Bolt(b *testing.B) {
	b.SetParallelism(1)

	take++
	t0 := time.Now()
	defer func() {
		log.Printf("take %d (n=%d) done in %v\n\n", take, b.N, time.Now().Sub(t0))
	}()

	log.Printf("bench get LARGE (8MB), take %d (n=%d)", take, b.N)
	benchGetBolt(b, 20*GB, LargeVal)
}

func benchGetBolt(b *testing.B, size uint, val []byte) {
	db := openboltEnv(dbPath)
	defer db.Close()

	txn, err := db.Begin(false)
	checkErr(err)

	bucket := txn.Bucket(benchBucket)

	b.SetBytes(int64(len(val)))
	b.ResetTimer()
	b.ReportAllocs()

	var found int
	var missed int
	for i := 0; i < b.N; i++ {
		// key := useKey(src.Intn(count + count/20))
		key := randKey()
		v := bucket.Get(key)
		if v == nil {
			missed++
		} else {
			found++
			if len(v) != len(val) {
				log.Fatalln("expected", len(val), "but got", len(v))
			}
		}
	}

	log.Println("missed:", missed, "found:", found)
	mem()
}
