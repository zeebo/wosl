package io

// Disk is an interface abstracting some persistent storage.
type Disk interface {
	// BlockSize returns the natural blocksize of the disk. Read and Write
	// are able to operate on arbitrary sizes, but perform the best when
	// operating on this size.
	BlockSize() int64

	// Read returns the data associated with the given block number. If there
	// is no data for that block number, nil is returned with no error. The
	// returned byte slices should have 64bit alignment (fresh Go allocated
	// memory already has this property. See sync/atomic documentation).
	Read(block int64) ([]byte, error)

	// Write stores the data for the given block number. It is an atomic
	// operation: subsequent reads will either see the result of the write
	// or the previous value.
	Write(block int64, data []byte) error
}
