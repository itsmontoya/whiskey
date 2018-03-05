# Whiskey

## Benchmarks

```bash
# go version go1.10 linux/amd64
goos: linux
goarch: amd64
pkg: github.com/itsmontoya/whiskey

# Whiskey
BenchmarkWhiskeyGet-16            2000          655140 ns/op        176008 B/op       7000 allocs/op
BenchmarkWhiskeyPut-16             100       775288107 ns/op        590635 B/op      19000 allocs/op
BenchmarkWhiskeyBatchPut-16       3000          442228 ns/op           575 B/op         19 allocs/op

# "github.com/boltDB/bolt"
BenchmarkBoltGet-16               1000         1531016 ns/op        488009 B/op       8000 allocs/op
BenchmarkBoltPut-16                  1      2362249290 ns/op      34075936 B/op      49951 allocs/op
BenchmarkBoltBatchPut-16           500         3814351 ns/op        161560 B/op       3098 allocs/op

# "github.com/bmatsuo/lmdb-go/lmdb"
BenchmarkLMDBGet-16               2000         1347530 ns/op        120054 B/op       5001 allocs/op
BenchmarkLMDBPut-16                  1      2272421139 ns/op        124312 B/op       4025 allocs/op
BenchmarkLMDBBatchPut-16           500         3141653 ns/op           132 B/op          4 allocs/op

```