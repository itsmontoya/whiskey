# Whiskey

## Benchmarks

```bash
# go version go1.10 linux/amd64
goos: linux
goarch: amd64
pkg: github.com/itsmontoya/whiskey

# Whiskey
BenchmarkWhiskeyGet-16            2000          847693 ns/op        176011 B/op       7000 allocs/op
BenchmarkWhiskeyPut-16             100        12207011 ns/op        720110 B/op      25000 allocs/op
BenchmarkWhiskeyBatchPut-16      10000          167264 ns/op           721 B/op         25 allocs/op

# "github.com/boltDB/bolt"
BenchmarkBoltGet-16               1000         1553200 ns/op        488010 B/op       8000 allocs/op
BenchmarkBoltPut-16                  1      2458374706 ns/op      34075920 B/op      49951 allocs/op
BenchmarkBoltBatchPut-16           300         3770269 ns/op        161626 B/op       3103 allocs/op

# "github.com/bmatsuo/lmdb-go/lmdb"
BenchmarkLMDBGet-16               1000         1400146 ns/op        120056 B/op       5001 allocs/op
BenchmarkLMDBPut-16                  1      2340215682 ns/op        124312 B/op       4025 allocs/op
BenchmarkLMDBBatchPut-16           500         3230487 ns/op           132 B/op          4 allocs/op
```