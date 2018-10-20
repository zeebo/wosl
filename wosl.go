package wosl

import (
	"math"

	"github.com/cespare/xxhash"
	"github.com/zeebo/errs"
	"github.com/zeebo/wosl/internal/node"
)

var Error = errs.Class("wosl")

const rootBlock uint32 = 1

// T is a write-optimized skip list. It is not thread safe.
type T struct {
	eps      float64
	disk     Disk
	roots    []*node.T // roots[0] is the lowest level
	maxBlock uint32

	b      int32  // block size from disk
	beps   uint32 // b^eps
	bneps  uint32 // b^(1 - eps)
	rBeps  uint32 // used for height calculation. expresses 1 / B^eps
	rBneps uint32 // used for height calculation. expresses 1 / B^(1 - eps)
}

// New returns a write-optmized skip list that uses the disk for reads and writes.
// The passed in epsilon argument must obey 0 < eps < 1, and must be the same for
// every call using the same disk.
func New(eps float64, disk Disk) (*T, error) {
	b := disk.BlockSize()
	beps := math.Pow(float64(b), eps)
	bneps := math.Pow(float64(b), 1-eps)
	rBeps := uint32(float64(math.MaxUint32) / beps)
	rBneps := uint32(float64(math.MaxUint32) / bneps)

	roots, err := loadRoots(disk)
	if err != nil {
		return nil, Error.Wrap(err)
	}
	if len(roots) == 0 { // first run gets a new node
		roots = []*node.T{node.New(b, 0)}
	}

	maxBlock, err := disk.MaxBlock()
	if err != nil {
		return nil, Error.Wrap(err)
	}

	return &T{
		eps:      eps,
		disk:     disk,
		roots:    roots,
		maxBlock: maxBlock,

		b:      b,
		beps:   uint32(beps),
		bneps:  uint32(bneps),
		rBeps:  rBeps,
		rBneps: rBneps,
	}, nil
}

// loadRoots loads the roots slice from the disk. It starts at the highest
// root entry (at the rootBlock index), and follows its pivot entry until
// the last one which has a zero pivot.
func loadRoots(disk Disk) ([]*node.T, error) {
	var roots []*node.T

	for block := rootBlock; block != 0; {
		buf, err := disk.Read(block)
		if err != nil {
			return nil, Error.Wrap(err)
		}

		if buf == nil {
			if block == 1 {
				break
			}
			return nil, Error.New("invalid pivot in root structure")
		}

		n, err := node.Load(buf)
		if err != nil {
			return nil, Error.Wrap(err)
		}

		roots = append(roots, n)
		block = n.Pivot()
	}

	// reverse so that roots[0] has the lowest height
	for i, j := 0, len(roots)-1; i < j; i, j = i+1, j-1 {
		roots[i], roots[j] = roots[j], roots[i]
	}

	return roots, nil
}

// Insert associates value with key in the skip list.
func (t *T) Insert(key, value []byte) error {
	// Compute the height for the key. It's important to realize that with
	// a block size of 4MB, and an eps of 0.5, then we expect the height of
	// the key to follow 1 / (2048 ^ h), so we can essentially be sure that
	// every key is in [0, 1, 2], and should optimize heavily for lower
	// heights being common.
	h := height(xxhash.Sum64(key), t.rBneps, t.rBeps)

	for h > int16(len(t.roots)-1) {
		// TODO(jeff): This should not be a for loop. That makes the
		// write problem described below EVEN WORSE! But, since this
		// is super rare, and this is the easiest way to make sure
		// it's correct, we tolerate it. Probably forever.

		// We need to allocate a new root. Since we know no one points into
		// the root, we just need to write the current root out as a new
		// node, allocate a new node with the new max height, and write it
		// out as block 1.
		buf := make([]byte, t.b)
		t.maxBlock++
		newBlock := t.maxBlock
		highest := t.roots[len(t.roots)-1]

		// TODO(jeff): the writes here are particularly egregious. specifically
		// the second one is writing approximately no data, but passes the
		// entire block off to the disk. it'd be nice to signal that we didn't
		// actually need all of that data. additionally, it doesn't even zero
		// it, so it's not going to be sparse, either.

		// First write out the highest node to a new block
		if err := highest.Write(buf); err != nil {
			return Error.Wrap(err)
		}
		if err := t.disk.Write(newBlock, buf); err != nil {
			return Error.Wrap(err)
		}

		// Allocate a new root and make the pivot point to the old root.
		root := node.New(t.b, 0)
		root.SetPivot(newBlock)

		// Write out the new root.
		if err := root.Write(buf); err != nil {
			return Error.Wrap(err)
		}
		if err := t.disk.Write(rootBlock, buf); err != nil {
			return Error.Wrap(err)
		}

		t.roots = append(t.roots, root)
	}

	// TODO(jeff): need to do some sort of LRU or something around nodes
	// rather than their buffers, since we spend time deserializing them.
	// The only part that's expensive is loading the entries slice. Maybe
	// we can either not do that during a load (either bloat the format
	// to add a uint32 pointer for each entry for O(1) seeks to the nth
	// entry or just keep every entry in memory?) and maybe just request
	// the buf from the disk every time?

	n := t.roots[h]
	if !n.Insert(key, value) {
		// TODO(jeff): Flush the node to the appropriate nodes.
		// just reset for now so that we can get some benchmarks.
		n.Reset()
		n.Insert(key, value)
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
	panic("not implemented")
}

// Successor returns the entry that sorts after key but still has the prefix
// if one exists. Otherwise, it returns nil, nil. It is not safe to modify the
// returned slices.
func (t *T) Successor(key, prefix []byte) ([]byte, []byte, error) {
	panic("not implemented")
}
