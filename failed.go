// +build ignore

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
	defer flushThunk.Start().Stop()

	// assert that we don't flush a leaf node.
	debug.Assert("flush leaf node", func() bool { return n.Height() != 0 })

	// special case: is this the root node at height 1, and there's
	// no leaf for it yet? if so, allocate a leaf node and put it
	// in the cache to be written back if dirtied.
	if n.Pivot() == invalidBlock {
		newBlock := t.maxBlock + 1
		t.cache.Add(node.New(0), newBlock)
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
	err := n.Flush(func(ent *node.Entry, key, value []byte) error {
		// if the entry is a pivot, then the child that we
		// flush to must be updated to it.
		if pivot := ent.Pivot(); pivot != 0 && cblock != pivot {
			cle.Close()
			cblock = pivot
			cfix = false
			cn = nil
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
		for _, spit := range splits {
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
			for _, fxup := range fixups {
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
	// TODO(jeff): rebalance needs to distribute the data in the children
	// based on the pivots in the parents, so that each child starts on
	// a pivot of the parents, and is approximately as big as a block.
	return nil
}
