package btree

import "github.com/zeebo/wosl/internal/node/entry"

// Bulk allows bulk loading of entries into a btree. they must
// be appended in strictly ascending order.
type Bulk struct {
	b   T
	n   *node
	nid uint32
}

// Append cheaply adds the entry to the btree. it must be strictly
// greater than any earlier entry.
func (b *Bulk) Append(ent entry.T) {
	if b.n == nil {
		b.n, b.nid = b.b.alloc(true)
		b.b.root, b.b.rid = b.n, b.nid
	}
	b.b.append(b.n, b.nid, ent)
}

// Length returns how many bytes a btree would be.
func (b *Bulk) Length() uint64 { return b.b.Length() }

// Done returns the bulk loaded btree.
func (b *Bulk) Done() T { return b.b }
