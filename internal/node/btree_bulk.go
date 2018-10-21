package node

// btreeBulk allows bulk loading of entries into a btree. they must
// be appended in strictly ascending order.
type btreeBulk struct {
	b   btree
	n   *btreeNode
	nid uint32
}

// append cheaply adds the entry to the btree. it must be strictly
// greater than any earlier entry.
func (b *btreeBulk) append(ent entry) {
	if b.n == nil {
		b.n, b.nid = b.b.alloc(true)
		b.b.root, b.b.rid = b.n, b.nid
	}
	b.b.append(b.n, b.nid, ent)
}

// done returns the bulk loaded btree.
func (b *btreeBulk) done() btree { return b.b }
