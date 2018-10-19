package caches

import "github.com/zeebo/wosl/io"

// T is an LRU cache for an io.Disk that implements the same interface. It is
// not thread safe.
type T struct {
	capacity int
	disk     io.Disk
}

// New returns an LRU cache that implements io.Disk with the given capacity.
func New(capacity int, disk io.Disk) *T {
	return &T{
		capacity: capacity,
		disk:     disk,
	}
}

// TODO(jeff): implement io.Disk but with an LRU cache in front.
