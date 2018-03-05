package whiskey

import (
	"testing"
)

func TestFreelist(t *testing.T) {
	var f freelist
	if offset := f.acquire(16); offset != -1 {
		t.Fatalf("invalid value, expected \"%v\" and received \"%v\"", -1, offset)
	}

	f.release(0, 16)

	if len(f.s) != 1 {
		t.Fatalf("invalid slice, %v", f.s)
	}

	f.release(32, 16)

	if len(f.s) != 2 {
		t.Fatalf("invalid slice, %v", f.s)
	}

	f.release(16, 16)

	if len(f.s) != 1 {
		t.Fatalf("invalid slice, %v", f.s)
	}

	if offset := f.acquire(8); offset != 0 {
		t.Fatalf("invalid value, expected \"%v\" and received \"%v\"", 0, offset)
	}

	if offset := f.acquire(16); offset != 8 {
		t.Fatalf("invalid value, expected \"%v\" and received \"%v\"", 8, offset)
	}

	if offset := f.acquire(28); offset != -1 {
		t.Fatalf("invalid value, expected \"%v\" and received \"%v\"", -1, offset)
	}

	if offset := f.acquire(24); offset != 24 {
		t.Fatalf("invalid value, expected \"%v\" and received \"%v\"", 24, offset)
	}

	if len(f.s) != 0 {
		t.Fatalf("invalid slice, %v", f.s)
	}
}
