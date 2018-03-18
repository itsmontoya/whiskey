package main

import (
	"fmt"
	"log"
	"os"
	"path"
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

func boltPut(count, batchSize, envSize int64, val []byte) {
	defer rmDB(dbPath)

	db := openboltEnv(dbPath)
	defer db.Close()

	txn, err := db.Begin(true)
	checkErr(err)

	bucket, err := txn.CreateBucket(benchBucket)
	checkErr(err)

	start := time.Now().UnixNano()

	for i := int64(0); i < count; i++ {
		key := randKey()
		checkErr(bucket.Put(key, val))
		if i%batchSize == 0 {
			checkErr(txn.Commit())
			txn, err = db.Begin(true)
			checkErr(err)
			bucket = txn.Bucket(benchBucket)
		}
	}

	checkErr(txn.Commit())

	end := time.Now().UnixNano()
	deltaMS := nsToMS(end - start)
	deltaS := float64(deltaMS) / 1000
	size := int64(len(val))
	sizeMB := float64(size) / MB
	throughput := (sizeMB * float64(count)) / deltaS

	fmt.Printf(reportFmt, "Bolt (PUT)", count, deltaMS, throughput)
}
