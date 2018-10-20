package wosl

// Disk is an interface abstracting some persistent storage.
type Disk interface {
	// BlockSize returns the natural blocksize of the disk. Read and Write
	// are able to operate on arbitrary sizes, but perform the best when
	// operating on this size.
	BlockSize() int32

	// Read returns the data associated with the given block number. If there
	// is no data for that block number, nil is returned with no error.
	Read(block uint32) ([]byte, error)

	// Write stores the data for the given block number. It is an atomic
	// operation: subsequent reads will either see the result of the write
	// or the previous value. The operation is also serial: later calls to
	// Write being observed implies that earlier calls to write will be
	// observed.
	Write(block uint32, data []byte) error

	// MaxBlock returns the id of the largest block written. It returns
	// zero if no blocks have been written.
	MaxBlock() (uint32, error)
}
