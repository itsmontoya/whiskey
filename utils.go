package whiskey

func getBucketKey(key []byte) (prepended []byte) {
	prepended = make([]byte, len(key)+1)
	prepended[0] = bucketPrefix
	copy(prepended[1:], key)
	return
}
