package node

type btreeBulk struct {
	b   btree      // btree
	n   *btreeNode // current node
	nid uint32     // current node's id
}

func (b *btreeBulk) append(ent entry) {
	if b.n == nil {
		b.n, b.nid = b.b.alloc(true)
		b.b.root, b.b.rid = b.n, b.nid
	}
	b.b.append(b.n, b.nid, ent)
}

func (b btreeBulk) done() btree { return b.b }
