# Glass

## Benchmarks
```bash
goos: linux
goarch: amd64
pkg: github.com/itsmontoya/whiskey/glass
# Whiskey
BenchmarkWhiskeyGet-16              2000            931809 ns/op          272011 B/op       7000 allocs/op
BenchmarkWhiskeyPut-16              1000           1986071 ns/op          376011 B/op      12000 allocs/op
BenchmarkWhiskeyBatchPut-16         2000            552805 ns/op             381 B/op         12 allocs/op

# "github.com/boltDB/bolt"
BenchmarkBoltGet-16                 1000           1573783 ns/op          488011 B/op       8000 allocs/op
BenchmarkBoltPut-16                    1        2379945623 ns/op        34075904 B/op      49951 allocs/op
BenchmarkBoltBatchPut-16             500           3846747 ns/op          161554 B/op       3098 allocs/op

# "github.com/bmatsuo/lmdb-go/lmdb"
BenchmarkLMDBGet-16                 1000           1452899 ns/op          120058 B/op       5001 allocs/op
BenchmarkLMDBPut-16                    1        2334212834 ns/op          124312 B/op       4025 allocs/op
BenchmarkLMDBBatchPut-16             500           3176090 ns/op             132 B/op          4 allocs/op
```