package node

import (
	"math"

	"github.com/zeebo/mon"
	"github.com/zeebo/wosl/internal/node/btree"
	"github.com/zeebo/wosl/internal/node/entry"
)

// Bulk allows for bulk loading data into a node, if it already exists
// in sorted order.
type Bulk struct {
	buf []byte
	bu  btree.Bulk
}

// Reset clears the state of the bulk import.
func (b *Bulk) Reset() {
	b.buf = nil
	b.bu = btree.Bulk{}
}

// Length returns an upper bound on how many bytes writing the
// eventually returned node would require.
func (b *Bulk) Length() uint64 {
	return 0 +
		nodeHeaderPadded +
		b.bu.Length() +
		uint64(len(b.buf)) +
		0
}

// Fits returns if a write for the given key would fit in size.
func (b *Bulk) Fits(key, value []byte, size uint32) bool {
	return len(key) <= entry.KeyMask &&
		len(value) <= entry.ValueMask &&
		// we add 10 btreeNodeSize to protect if the insert would cause a split
		// which might allocate up to log(n) nodes. there's no way that's ever
		// bigger than 10 (famous last words).
		b.Length()+10*btree.NodeSize < uint64(size)
}

var bulkAppendThunk mon.Thunk // timing info for bulk.Append

// Append adds the key/value to the bulk importer. If tombstone
// is true, it is added as a tombstone. It returns true if the
// write happened, and false if it would cause the node to become
// too large.
func (b *Bulk) Append(key, value []byte, tombstone bool, pivot uint32) bool {
	timer := bulkAppendThunk.Start()

	// make sure the write is ok to go
	if !b.Fits(key, value, math.MaxUint32) {
		timer.Stop()
		return false
	}

	// build the entry that we will insert.
	ent := entry.New(key, value, tombstone, uint32(len(b.buf)))
	ent.SetPivot(pivot)

	// add the data to the buffer
	b.buf = append(b.buf, key...)
	b.buf = append(b.buf, value...)

	// insert it into the bulk loader.
	b.bu.Append(ent)

	timer.Stop()
	return true
}

// Done returns a node with the given next and height using the
// bulk loaded data. It should not be called multiple times.
func (b *Bulk) Done(height uint32) *T {
	t := New(height)
	t.buf = b.buf
	t.entries = b.bu.Done()
	t.dirty = t.entries.Count() > 0
	return t
}
