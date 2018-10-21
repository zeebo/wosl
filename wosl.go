package wosl

import (
	"math"

	"github.com/cespare/xxhash"
	"github.com/zeebo/errs"
	"github.com/zeebo/wosl/internal/debug"
	"github.com/zeebo/wosl/internal/mon"
	"github.com/zeebo/wosl/internal/node"
	"github.com/zeebo/wosl/lease"
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
		root = node.New(0, 1)
		root.SetPivot(invalidBlock)
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

	// TODO(jeff): log ahead the values into some journal per block.
	// or something. somewhere needs to handle it. for now, just
	// record the hash of the value lol.
	val := xxhash.Sum64(value) >> 20

	// Compute the height for the key to check if we need to allocate
	// new roots. This should be exceedinly rare, so it's ok if it's
	// slightly inefficient.
	h := t.height(key)
	for h >= t.root.Height() {
		if err := t.newRoot(); err != nil {
			timer.Stop()
			return Error.Wrap(err)
		}
	}

	// do a recursive/flushing insert of the key starting at the root.
	if _, err := t.insert(t.root, rootBlock, key, val); err != nil {
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

	t.root = node.New(0, t.root.Height()+1)
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

// insert places the key/value into the node at the given block. If the node does not
// contain enough space, it is flushed, recursively inserting elements into its children.
func (t *T) insert(n *node.T, block uint32, key []byte, val uint64) (bool, error) {
	// if we're smaller than the
	if n.Fits(key, t.b) && n.Insert(key, val) {
		return false, nil
	}

	// if we're a leaf the parent calling insert needs to handle the
	// flush specially.
	if n.Height() == 0 {
		return true, nil
	}

	// TODO(jeff): have to be VERY careful about concurrency here. for now
	// assume everything is serialized behind a mutex. there also may be
	// some ways to flush less. gotta figure that one out. it also may be
	// better to flush AFTER the insertion. gotta figure that out, too.

	// we have to flush the root. first write back all of the dirty data.
	if n == t.root {
		if err := t.cache.Flush(); err != nil {
			return false, Error.Wrap(err)
		}
	}

	// otherwise, flush and attempt another insert.
	if err := t.flush(n, block); err != nil {
		return false, Error.Wrap(err)
	} else if !n.Insert(key, val) {
		return false, Error.New("entry too large to fit")
	} else {
		return false, nil
	}
}

var flushThunk mon.Thunk // timing for flush

// flush distributes the elements of the node's buffer among the
// buffers of the node's children. It should not be called on leaves.
func (t *T) flush(n *node.T, block uint32) error {
	timer := flushThunk.Start()

	// assert that we don't flush a leaf node.
	debug.Assert("flush leaf node", func() bool { return n.Height() != 0 })

	// special case: is this the root node at height 1, and there's
	// no leaf for it yet? if so, allocate a leaf node and put it
	// in the cache to be written back if dirtied.
	if n.Pivot() == invalidBlock {
		block := t.maxBlock + 1
		t.cache.Add(node.New(0, 0), block)
		n.SetPivot(block)
		t.maxBlock++
	}

	// TODO(jeff): is it worthwhile to have entries pack in the height
	// of the key so that we don't have to recompute it? we're going to
	// be reading the key anyway, and computing the hash is like
	// nanoseconds, so it won't matter. maybe it does!?

	var ( // state for the iteration
		le    lease.T
		child = n.Pivot()
		nh    = n.Height()
	)

	err := n.IterKeys(func(key []byte, pivot uint32) (uint32, error) {
		// update which child node we will insert the entry into
		if pivot != 0 && child != pivot {
			le.Close()
			child = pivot
		}
		if le.Zero() {
			var err error
			le, err = t.cache.Get(child)
			if err != nil {
				return 0, Error.Wrap(err)
			}
		}

		// insert the entry into the
		h := t.height(key)
		if nh <= h { // make this entry a pivot for the child
			pivot = child
		}

		return pivot, nil
	})
	le.Close()

	timer.Stop()
	return Error.Wrap(err)
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
