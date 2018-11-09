package wosl

import (
	"github.com/zeebo/wosl/internal/debug"
	"github.com/zeebo/wosl/internal/node"
	"github.com/zeebo/wosl/lease"
)

func (t *T) flush(n *node.T, block uint32, parents []uint32) (*node.T, []uint32, error) {
	nh := n.Height()

	debug.Assert("flush must not happen on leaf", func() bool { return nh > 0 })
	if nh == 1 {
		return nil, nil, t.rebalance(n, block, parents)
	}

	cblock := n.Pivot()
	child, err := t.cache.Get(cblock)
	if err != nil {
		return nil, nil, Error.Wrap(err)
	}

	var (
		children = []lease.T{child}
		splits   []*node.T
		bulk     node.Bulk
	)

	// upon exit, clean up leases on the children
	defer func() {
		for _, child := range children {
			// TODO(jeff): how to handle this error?
			child.Close()
		}
	}()

	// walk all the entries and rebuild the node into possibly multiple nodes
	iter := n.Iterator()
	for iter.Next() {
		key, value, ent := iter.Key(), iter.Value(), iter.Entry()
		pivot := ent.Pivot()
		he := t.height(key)

		// if the entry has a pivot, move to inserting into that child
		if pivot > 0 {
			child, err = t.cache.Get(pivot)
			if err != nil {
				return nil, nil, Error.Wrap(err)
			}
			children = append(children, child)
			cblock = pivot
		}

		// if the node height is <= the entry height, it becomes a pivot
		// for the child.
		if nh <= he {
			pivot = cblock
		}

		// TODO(jeff): this api sucks
		if ent.Tombstone() {
			if !child.Node().Delete(key) {
				return nil, nil, Error.New("entry too large to fit")
			}
		} else {
			if !child.Node().Insert(key, value, pivot) {
				return nil, nil, Error.New("entry too large to fit")
			}
		}

		// perform a split if the entry height is strictly greater
		if nh < he {
			fin := bulk.Done(nh)
			fin.SetPivot(cblock)
			splits = append(splits, fin)
			bulk.Reset()
		}

		bulk.Append(key, value, ent.Tombstone(), pivot)
	}

	// fin is the furthest right node in our splits
	fin := bulk.Done(nh)
	fin.SetPivot(cblock)
	splits = append(splits, fin)

	// set the next pointers and allocate blocks.
	prev := splits[0]
	_ = prev

	// return the set of children we flushed to so that they can be
	// flushed. we don't do this recursively to avoid holding a bunch
	// of leases for too long.
	flushed := make([]uint32, len(children))
	for i, child := range children {
		flushed[i] = child.Block()
	}
	return splits[0], flushed, nil
}

func (t *T) rebalance(n *node.T, block uint32, parents []uint32) error {
	debug.Assert("rebalance on height 1", func() bool { return n.Height() == 1 })

	return nil
}
