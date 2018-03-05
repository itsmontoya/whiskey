package whiskey

import (
	"bytes"
	"os"
	"testing"
)

func TestBackend(t *testing.T) {
	var (
		a   *allocator
		b   *backend
		err error
	)

	if err = os.MkdirAll("./testing", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./testing")

	if a, err = newallocator("./testing", "allocator", RW, InitialSize); err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	b = newbackend(a)
	bs := b.Grow(8)
	bs[0] = 0
	bs[1] = 1
	bs[2] = 2
	bs[3] = 3
	bs[4] = 4
	bs[5] = 5
	bs[6] = 6
	bs[7] = 7

	nb := b.dup()
	if !bytes.Equal(bs, nb.bs) {
		t.Fatalf("Invalid value, expected \"%v\" and received \"%v\"", bs, nb.bs)
	}
}
