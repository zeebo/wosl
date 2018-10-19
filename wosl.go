package wosl

import "github.com/zeebo/wosl/io"

// T is a write-optimized skip list. It is not thread safe.
type T struct {
	eps  float64
	disk io.Disk
}

// New returns a write-optmized skip list that uses the disk for reads and writes.
// The passed in epsilon argument must obey 0 < eps < 1, and must be the same for
// every call using the same disk.
func New(eps float64, disk io.Disk) *T {
	return &T{
		eps:  eps,
		disk: disk,
	}
}

// Insert associates value with key in the skip list. It is not safe to
// modify the key or value slices.
func (t *T) Insert(key, value []byte) error {
	panic("not implemented")
}

// Read returns the data for k if it exists. Otherwise, it returns nil. It is
// not safe to modify the returned slice.
func (t *T) Read(key []byte) ([]byte, error) {
	panic("not implemented")
}

// Delete removes the key from the skip list. It is not safe to modify the
// key slice.
func (t *T) Delete(key []byte) error {
	panic("not implemented")
}

// Successor returns the entry that sorts after key but still has the prefix
// if one exists. Otherwise, it returns nil, nil. It is not safe to modify the
// returned slices.
func (t *T) Successor(key, prefix []byte) ([]byte, []byte, error) {
	panic("not implemented")
}
