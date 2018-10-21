package wosl

import (
	"github.com/zeebo/wosl/internal/node"
	"github.com/zeebo/wosl/lease"
)

// Cache is an interface around a cache of nodes.
type Cache interface {
	// Disk returns the backing disk of the cache.
	Disk() Disk

	// Flush writes back all of the dirty nodes that are leased
	// or remaining in the cache.
	Flush() error

	// Get retreives the node at the given block. It will return
	// an error if there is no node for that block.
	Get(block uint32) (lease.T, error)

	// Add places the node in the cache with the given block. It
	// is undefined (though may panic) if a node is added for a
	// block number that already exists in the cache.
	Add(n *node.T, block uint32)
}

// Disk is an interface abstracting some persistent storage.
type Disk interface {
	// BlockSize returns the natural blocksize of the disk. Read and Write
	// are able to operate on arbitrary sizes, but perform the best when
	// operating on this size.
	BlockSize() uint32

	// Read returns the data associated with the given block number. If there
	// is no data for that block number, nil is returned with no error.
	Read(block uint32) ([]byte, error)

	// Write stores the data for the given block number. It is an atomic
	// operation: subsequent reads will either see the result of the write
	// or the previous value. The operation is also serial: later calls to
	// Write being observed implies that earlier calls to Write or
	// Delete will be observed, across all block numbers.
	Write(block uint32, data []byte) error

	// Delete removes the block for the given block nunber. It is an
	// atomic operation: subsequent reads will either see the block or
	// no block at all. The operation is also serial: later calls to
	// Delete being observed implies that all earlier calls to Write
	// or Delete will be observed, across all block numbers. It does
	// not return an error if the block does not exist.
	Delete(block uint32) error

	// MaxBlock returns the id of the largest block ever written. It returns
	// zero if no blocks have been written. It does not decrease, even
	// if the maximum block has been deleted.
	MaxBlock() (uint32, error)
}
