package lease

import "github.com/zeebo/wosl/internal/node"

// T is a lease on a node from some block. It is used to track how
// long some node is in use.
type T struct {
	n     *node.T
	block uint32
	cb    func(*node.T, uint32) error
}

// New constructs a lease for a node/block that will have the callback
// called with the node/block when Close is called.
func New(n *node.T, block uint32, cb func(*node.T, uint32) error) T {
	return T{
		n:     n,
		block: block,
		cb:    cb,
	}
}

// Zero returns if the lease is the zero value.
func (t T) Zero() bool { return t.cb == nil }

// Node returns the node associated with the lease.
func (t T) Node() *node.T { return t.n }

// SetNode updates the node associated with the lease.
func (t *T) SetNode(n *node.T) { t.n = n }

// Block returns the block number the node was retreived with.
//
// N.B. This may not be the current block number in the case of
// the root node, but the root should not be retreived through
// the cache, anyway.
func (t T) Block() uint32 { return t.block }

// Close releases the resources associated with the lease. It
// clears out any state so that it becomes a zero lease. The
// state is cleared regardless of if an error is returned.
func (t *T) Close() (err error) {
	if t.cb != nil {
		err = t.cb(t.n, t.block)
	}
	*t = T{}
	return err
}
