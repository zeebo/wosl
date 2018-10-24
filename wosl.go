package wosl

import (
	"bytes"
	"fmt"
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

	fmt.Println("insert", t.height(key), string(key))

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

	// we're going to flush the root, write back all of the dirty data.
	if err := t.cache.Flush(); err != nil {
		timer.Stop()
		return Error.Wrap(err)
	}

	// we don't have access to the parent yet, but that's ok for the
	// first time around, since we know the first insert is due to the
	// root, and the root never has to split or has any incoming edges
	// that need to be fixed.
	fmt.Println("start flush")
	if err := t.flush(t.root, rootBlock, nil); err != nil {
		timer.Stop()
		return Error.Wrap(err)
	}
	fmt.Println("end flush")

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

var flushThunk mon.Thunk // timing for flush

// flush distributes the elements of the node's buffer among the
// buffers of the node's children. If the flush causes any splits
// it will fix up the pointers to entries inside of the node in
// the provided parents slice. Any children that it flushed to
// that went over the block size are then recursively flushed.
// There is a special flushing strategy for nodes at height
// of 1 where instead of recrusively flushing the leaves, they
// are rebalanced based on the pivots of the node.
func (t *T) flush(n *node.T, block uint32, parents []uint32) error {
	fmt.Println("flush", block, n.Height())

	defer flushThunk.Start().Stop()

	// assert that we don't flush a leaf node.
	debug.Assert("flush leaf node", func() bool { return n.Height() != 0 })

	// special case: is this the root node at height 1, and there's
	// no leaf for it yet? if so, allocate a leaf node and put it
	// in the cache to be written back if dirtied.
	if n.Pivot() == invalidBlock {
		newBlock := t.maxBlock + 1
		t.cache.Add(node.New(0, 0), newBlock)
		n.SetPivot(newBlock)
		t.maxBlock++
	}

	// TODO(jeff): is it worthwhile to have entries pack in the height
	// of the key so that we don't have to recompute it? we're going to
	// be reading the key anyway, and computing the hash is like
	// nanoseconds, so it won't matter. maybe it does!?

	type split struct {
		block  uint32
		n      *node.T
		leader []byte
	}

	var ( // state for the iteration
		cle    lease.T     // current child lease
		cblock = n.Pivot() // block of current child
		cfix   = false     // if the child needs fixing
		cn     *node.T     // current child node

		nh       = n.Height() // height of the current node
		fixups   []uint32     // which children we need to fix up
		leader   []byte       // leader that caused the start of the split
		splits   []split      // which splits have happened
		bulk     node.Bulk    // building any splits
		maxBlock = t.maxBlock // current max block
	)

	fmt.Println("initial child", block, "->", cblock)

	err := n.Flush(func(ent *node.Entry, key, value []byte) error {
		// if the entry is a pivot, then the child that we
		// flush to must be updated to it.
		if pivot := ent.Pivot(); pivot != 0 && cblock != pivot {
			cle.Close()
			cblock = pivot
			cfix = false
			cn = nil
			fmt.Println("set child", string(key), t.height(key), block, "->", pivot)
		}

		// if we have no lease, get one so that we can flush the
		// entry to the child if necessary.
		if cn == nil {
			var err error
			cle, err = t.cache.Get(cblock)
			if err != nil {
				return Error.Wrap(err)
			}
			cn = cle.Node()
		}

		// insert the data into the child node. if it causes it to
		// be larger than the block size, record that we'll need
		// to fix it up after the flush. if we're at height 1, we
		// need to do a rebalance step at the end, so keep track
		// of every node that we inserted into.
		if !cn.Insert(key, value) {
			return Error.New("entry too large to fit")
		}
		if !cfix && (cn.Length() >= uint64(t.b) || nh == 1) {
			cfix = true
			fixups = append(fixups, cblock)
		}

		// TODO(jeff): how do we handle merges? should check
		// if the entry is a tombstone, and handle that.
		// TODO(jeff): if some child has enough delete entries
		// destined for it, we need to immediately flush.

		// if the height of the entry isgreater than or equal to
		// our node's height, we should make the entry a pivot to
		// the child. if the height is strictly greater, we need
		// to split.
		if h := t.height(key); h >= nh {
			ent.SetPivot(cblock)
			fmt.Println("flush pivot", string(key), h, "from", block, "->", cblock)

			if h > nh {
				// if we already have a split in progress, finish
				// it off and save it
				if leader != nil {
					maxBlock++
					splits = append(splits, split{
						block:  maxBlock,
						n:      bulk.Done(n.Next(), n.Height()),
						leader: leader,
					})
					bulk.Reset()
				}

				// update the leader that started the split
				leader = key
			}
		}

		// if we're in a split, append it to the current bulk node
		// and then set the pivot to zero to truncate it from our filter.
		if leader != nil {
			if !bulk.Append(key, value, ent.Tombstone(), ent.Pivot()) {
				return Error.New("entry too large to fit")
			}
			ent.SetPivot(0)
		}

		return nil
	})
	cle.Close()
	if err != nil {
		return Error.Wrap(err)
	}

	// if we're in a split, finish up the split state.
	if leader != nil {
		maxBlock++
		splits = append(splits, split{
			block:  maxBlock,
			n:      bulk.Done(n.Next(), n.Height()),
			leader: leader,
		})

		// add the new split nodes to the cache
		for _, split := range splits {
			fmt.Println("adding", split.block, "from bulk split")
			t.cache.Add(split.n, split.block)
		}

		// loop over all of the parents and fix any pivots to the current
		// node to point at any split nodes if necessary.
		pivot, tmpSplits := block, splits
		for _, parent := range parents {
			le, err := t.cache.Get(parent)
			if err != nil {
				return Error.Wrap(err)
			}
			le.Node().Update(func(ent *node.Entry, key []byte) bool {
				// if the pivot does not point at us, it's not part
				// of the split, so just continue.
				if ent.Pivot() != block {
					return true
				}

				// if there are splits to consume left, check to see if the key
				// has moved to the next leader. if so,
				if len(tmpSplits) > 0 && bytes.Compare(key, tmpSplits[0].leader) >= 0 {
					pivot, tmpSplits = tmpSplits[0].block, tmpSplits[1:]
				}

				fmt.Println("split pivot", string(key), t.height(key), "from", parent, "->", pivot)
				ent.SetPivot(pivot)
				return true
			})
			le.Close()
		}

		// update the maxBlock since we allocated some blocks
		t.maxBlock = maxBlock
	}

	// run any fixups that are needed.
	if len(fixups) > 0 {
		fmt.Println("start fixup")

		// now that we have some splits, we have to update the set of
		// parents for all of the children we're fixing up.
		parents := make([]uint32, 1, 1+len(splits))
		parents[0] = block
		for _, split := range splits {
			parents = append(parents, split.block)
		}

		if nh == 1 {
			if err := t.rebalance(parents, fixups); err != nil {
				return Error.Wrap(err)
			}
		} else {
			for _, fixup := range fixups {
				fmt.Println("fixup", fixup)
				le, err := t.cache.Get(fixup)
				if err != nil {
					return Error.Wrap(err)
				}

				err = t.flush(le.Node(), le.Block(), parents)
				le.Close()

				if err != nil {
					return Error.Wrap(err)
				}
			}
		}

		fmt.Println("end fixup")
	}

	// TODO(jeff): there are issues with partial failures and the next pointer
	// we need to flush every node in reverse order so that we don't update
	// the first node's next pointer until the end. additionally, the parent
	// pointer updates must happen after.

	return nil
}

// rebalance distributes elements in the children according to the pivots
// in the parents.
func (t *T) rebalance(parents, children []uint32) error {
	fmt.Println("rebalance", parents, children)
	// TODO(jeff): rebalance needs to distribute the data in the children
	// based on the pivots in the parents, so that each child starts on
	// a pivot of the parents, and is approximately as big as a block.
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
