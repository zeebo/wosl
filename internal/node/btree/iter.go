package btree

import "github.com/zeebo/wosl/internal/node/entry"

// Iterator walks over the entries in a btree.
type Iterator struct {
	b *T
	n *node
	i uint16
}

// Next advances the iterator and returns true if there is an entry.
func (i *Iterator) Next() bool {
	if i.n == nil {
		return false
	}
	i.i++

next:
	if i.i < i.n.count {
		return true
	}

	if i.n.next == invalidNode {
		i.n = nil
		return false
	}

	i.n = i.b.nodes[i.n.next]
	i.i = 0
	goto next
}

// Entry returns the current entry. It is only valid to call this
// if the most recent call to Next returned true.
func (i *Iterator) Entry() entry.T {
	return i.n.payload[i.i]
}
