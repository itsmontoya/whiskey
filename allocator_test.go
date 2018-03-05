package whiskey

import (
	"os"
	"testing"
)

func TestAllocator(t *testing.T) {
	var (
		a   *allocator
		err error
	)

	if err = os.MkdirAll("./testing", 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./testing")

	if a, err = newallocator("./testing", "allocator", RW, InitialSize); err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	bs, offset, _ := a.allocate(16)
	bs[3] = 67

	nbs, noffset, _ := a.allocate(16)
	nbs[1] = 13

	bs = a.mm[offset : offset+16]
	if bs[3] != 67 {
		t.Fatalf("invalid value, expected \"%d\" and received \"%d\"", 67, bs[3])
	}

	if err = a.Close(); err != nil {
		t.Fatal(err)
	}

	if a, err = newallocator("./testing", "allocator", RW, InitialSize); err != nil {
		t.Fatal(err)
	}

	bs = a.mm[offset : offset+16]
	if bs[3] != 67 {
		t.Fatalf("invalid value, expected \"%d\" and received \"%d\"", 67, bs[3])
	}

	nbs = a.mm[noffset : noffset+16]
	if nbs[1] != 13 {
		t.Fatalf("invalid value, expected \"%d\" and received \"%d\"", 13, nbs[1])
	}

	tbs, _, _ := a.allocate(16)
	tbs[2] = 24

	bs = a.mm[offset : offset+16]
	if bs[3] != 67 {
		t.Fatalf("invalid value, expected \"%d\" and received \"%d\"", 67, bs[3])
	}

	nbs = a.mm[noffset : noffset+16]
	if nbs[1] != 13 {
		t.Fatalf("invalid value, expected \"%d\" and received \"%d\"", 13, nbs[1])
	}
}
