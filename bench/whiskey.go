package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/itsmontoya/whiskey"
)

func openWhiskeyEnv(dbPath string, size int64) *whiskey.DB {
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		log.Fatalln(err)
	}

	db, err := whiskey.NewWithSize(dbPath, "benchmark", size)
	if err != nil {
		log.Fatalln(err)
	}

	return db
}

func whiskeyPut(count, batchSize, envSize int64, val []byte) {
	defer rmDB(dbPath)
	db := openWhiskeyEnv(dbPath, envSize)
	defer db.Close()

	txn, txnClose, err := db.UpdateTxn()
	checkErr(err)

	bucket, err := txn.CreateBucket(benchBucket)
	checkErr(err)

	start := time.Now().UnixNano()

	for i := int64(0); i < count; i++ {
		key := randKey()
		checkErr(bucket.Put(key, val))
		if i%batchSize == 0 {
			txnClose(true)
			txn, txnClose, err = db.UpdateTxn()
			checkErr(err)
			bucket, err = txn.Bucket(benchBucket)
			checkErr(err)
		}
	}

	txnClose(true)

	end := time.Now().UnixNano()
	deltaMS := nsToMS(end - start)
	deltaS := float64(deltaMS) / 1000
	size := int64(len(val))
	sizeMB := float64(size) / MB
	throughput := (sizeMB * float64(count)) / deltaS

	fmt.Printf(reportFmt, "Whiskey (PUT)", count, deltaMS, throughput)
}

const reportFmt = `
Benchmark finished
========================
Name: %s
# of iterations: %d
Time taken: %dms
Throughput: %vmb per second

`

func nsToMS(ns int64) int64 {
	return ns / 1000000
}
