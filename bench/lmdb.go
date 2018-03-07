package main

import (
	"fmt"
	"log"
	"os"
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

func lmdbPut(count, batchSize, envSize int64, val []byte) {
	defer rmDB(dbPath)
	env := openLMDBEnv(dbPath)
	defer env.Close()

	checkErr(env.SetMapSize(uint(envSize)))
	txn, err := env.BeginTxn(nil, lmdb.DefaultTxnFlags)
	checkErr(err)
	bucket, err := txn.DbiOpen(string(benchBucket), lmdb.DbiCreate)
	checkErr(err)

	start := time.Now().UnixNano()

	for i := int64(0); i < count; i++ {
		key := randKey()
		checkErr(txn.Put(bucket, key, val, 0))
		if i%batchSize == 0 {
			checkErr(txn.Commit())
			txn, err := env.BeginTxn(nil, lmdb.DefaultTxnFlags)
			checkErr(err)
			bucket, err = txn.DbiOpen(string(benchBucket), lmdb.DefaultDbiFlags)
			checkErr(err)
		}
	}

	checkErr(txn.Commit())

	end := time.Now().UnixNano()
	deltaMS := nsToMS(end - start)
	deltaS := float64(deltaMS) / 1000
	size := int64(len(val))
	sizeMB := float64(size) / MB
	throughput := (sizeMB * float64(count)) / deltaS

	fmt.Printf(reportFmt, "LMDB (PUT)", count, deltaMS, throughput)
}
