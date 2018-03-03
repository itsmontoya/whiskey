package whiskey

// Txn is a basic transaction interface
type Txn interface {
	grow(key []byte, sz int64) []byte

	Bucket(key []byte) (*Bucket, error)
	CreateBucket(key []byte) (*Bucket, error)

	Get(key []byte) (value []byte, err error)
	Put(key, value []byte) error
	Delete(key []byte) error
}

// TxnFn is called during transactions
type TxnFn func(txn Txn) error
