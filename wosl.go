package wosl

import (
	"math"

	"github.com/cespare/xxhash"
	"github.com/zeebo/errs"
	"github.com/zeebo/wosl/internal/mon"
	"github.com/zeebo/wosl/internal/node"
)

var Error = errs.Class("wosl")

const (
	noBlock      uint32 = 0
	rootBlock    uint32 = 1
	invalidBlock uint32 = math.MaxUint32
)

// T is a write-optimized skip list. It is not thread safe.
type T struct {
	eps     float64
	cache   Cache
	disk    Disk
	root    *node.T
	scratch []byte

	maxBlock uint32 // largest stored block from disk
	b        uint32 // block size from disk
	beps     uint32 // b^eps
	bneps    uint32 // b^(1 - eps)
	rBeps    uint32 // used for height calculation. expresses 1 / B^eps
	rBneps   uint32 // used for height calculation. expresses 1 / B^(1 - eps)
}

// New returns a write-optimized skip list that uses the cache for reads and writes.
// It uses an epsilon of 0.5, providing a balance between write and query performance
// such that queries are like a B-tree, and writes are much faster.
func New(cache Cache) (*T, error) {
	return NewEps(0.5, cache)
}

// NewEps returns a write-optimized skip list that uses the cache for reads and writes.
// The passed in epsilon argument must obey 0 < eps < 1, and must be the same for
// every call that uses the same backing store.
func NewEps(eps float64, cache Cache) (*T, error) {
	disk := cache.Disk()
	maxBlock, err := disk.MaxBlock()
	if err != nil {
		return nil, Error.Wrap(err)
	}

	// precompute some ratios for getting the height
	b := disk.BlockSize()
	beps := math.Pow(float64(b), eps)
	bneps := math.Pow(float64(b), 1-eps)
	rBeps := uint32(float64(math.MaxUint32) / beps)
	rBneps := uint32(float64(math.MaxUint32) / bneps)

	// load or create the root node
	var root *node.T
	if buf, err := disk.Read(rootBlock); err != nil {
		return nil, Error.Wrap(err)
	} else if buf == nil {
		root = node.New(1)
		root.SetPivot(invalidBlock)
		maxBlock = 1
	} else if root, err = node.Load(buf); err != nil {
		return nil, Error.Wrap(err)
	}

	return &T{
		eps:     eps,
		cache:   cache,
		disk:    disk,
		root:    root,
		scratch: make([]byte, b),

		maxBlock: maxBlock,
		b:        b,
		beps:     uint32(beps),
		bneps:    uint32(bneps),
		rBeps:    rBeps,
		rBneps:   rBneps,
	}, nil
}

// height returns the height of the key.
func (t *T) height(key []byte) uint32 {
	return height(xxhash.Sum64(key), t.rBneps, t.rBeps)
}

var insertThunk mon.Thunk // timing for Insert

// Insert associates value with key in the skip list.
func (t *T) Insert(key, value []byte) error {
	timer := insertThunk.Start()

	// Compute the height for the key to check if we need to allocate
	// new roots. This should be very rare, so it's ok if it's somewhat
	// inefficient.
	h := t.height(key)
	for h >= t.root.Height() {
		if err := t.newRoot(); err != nil {
			timer.Stop()
			return Error.Wrap(err)
		}
	}

	// insert the value. if it cannot be fit, then there's nothing to do.
	if !t.root.Insert(key, value) {
		timer.Stop()
		return Error.New("entry too large to fit")
	}

	// if we're still inside the block range, we're done!
	if t.root.Length() < uint64(t.b) {
		timer.Stop()
		return nil
	}

	// flush the root and any children that are required. it doesn't need to
	// have a slice of parents because it can't possibly split.
	if err := t.flush(t.root, rootBlock, nil); err != nil {
		timer.Stop()
		return Error.Wrap(err)
	}

	timer.Stop()
	return nil
}

// newRoot allocates and writes out a new root to the rootBlock, having it
// point to the current root (which is written to some other new block).
func (t *T) newRoot() error {
	block, err := t.writeNewNode(t.root)
	if err != nil {
		return Error.Wrap(err)
	}

	t.cache.Add(t.root, block)
	t.root = node.New(t.root.Height() + 1)
	t.root.SetPivot(block)
	return nil
}

// writeNewNode saves the node to disk and returns the block number
// it was written with.
func (t *T) writeNewNode(n *node.T) (uint32, error) {
	block := t.maxBlock + 1
	if err := t.writeNode(n, block); err != nil {
		return 0, Error.Wrap(err)
	}
	t.maxBlock = block
	return block, nil
}

// writeNode saves the node to the given block.
func (t *T) writeNode(n *node.T, block uint32) error {
	var err error
	if t.scratch, err = n.Write(t.scratch[:0]); err != nil {
		return Error.Wrap(err)
	} else if err := t.disk.Write(block, t.scratch); err != nil {
		return Error.Wrap(err)
	}
	return nil
}

// Read returns the data for k if it exists. Otherwise, it returns nil. It is
// not safe to modify the returned slice.
func (t *T) Read(key []byte) ([]byte, error) {
	panic("not implemented")
}

// Delete removes the key from the skip list. It is not safe to modify the
// key slice.
func (t *T) Delete(key []byte) error {
	// TODO(jeff): if some child has enough delete entries
	// destined for it, we need to immediately flush.

	panic("not implemented")
}

// Successor returns the entry that sorts after key but still has the prefix
// if one exists. Otherwise, it returns nil, nil. It is not safe to modify the
// returned slices.
func (t *T) Successor(key, prefix []byte) ([]byte, []byte, error) {
	panic("not implemented")
}
