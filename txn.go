package whiskey

// Txn is a basic transaction interface
type Txn interface {
	bucket(key []byte) (*Bucket, error)
	createBucket(key []byte) (*Bucket, error)

	get(key []byte) (value []byte, err error)
	put(key, value []byte) error
	delete(key []byte) error

	Bucket(key []byte) (*Bucket, error)
	CreateBucket(key []byte) (*Bucket, error)

	Get(key []byte) (value []byte, err error)
	Put(key, value []byte) error
	Delete(key []byte) error
}

// TxnFn is called during transactions
type TxnFn func(txn Txn) error
