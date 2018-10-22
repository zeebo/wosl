package node

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/zeebo/wosl/internal/mon"
)

const (
	invalidNode    = math.MaxUint32
	payloadEntries = 31 // 512 byte nodes
	payloadSplit   = payloadEntries / 2
)

// compare is like bytes.Compare but for uint32s.
func compare(a, b uint32) int {
	if a == b {
		return 0
	} else if a < b {
		return -1
	}
	return 1
}

// N.B. it is important that btreeNode does not contain pointers, so that we can
// construct arrays of them off heap.

// btreeNode are nodes in the btree.
type btreeNode struct {
	next    uint32  // pointer to the next node (or if not leaf, the rightmost edge)
	prev    uint32  // backpointer from next node (unused if not leaf)
	parent  uint32  // set to invalidNode on the root node
	count   uint8   // used values in payload
	leaf    bool    // set if is a leaf
	_       [2]byte // padding
	payload [payloadEntries]entry
}

// insertEntry inserts the entry into the node. it should never be called
// on a node that would have to split. it returns true if the count increased.
func (n *btreeNode) insertEntry(key []byte, ent entry, buf []byte) bool {
	prefix := binary.BigEndian.Uint32(ent.prefix[:])

	// binary search to find the appropriate child
	i, j := uint8(0), n.count
	for i < j {
		h := (i + j) >> 1
		enth := n.payload[h]
		prefixh := binary.BigEndian.Uint32(enth.prefix[:])

		switch compare(prefix, prefixh) {
		case 1:
			i = h + 1
		case 0:
			kh := enth.readKey(buf)
			switch bytes.Compare(key, kh) {
			case 1:
				i = h + 1
			case 0: // found a match. overwite and exit
				n.payload[h] = ent
				return false
			case -1:
				j = h
			}
		case -1:
			j = h
		}
	}

	copy(n.payload[i+1:], n.payload[i:n.count])
	n.payload[i] = ent
	n.count++
	return true
}

// appendEntry appends the entry into the node. it must compare greater than any
// element inside of the node, already, and should never be called on a node that
// would have to split.
func (n *btreeNode) appendEntry(ent entry) {
	n.payload[n.count] = ent
	n.count++
}

// btree is an in memory B+ tree tuned to store entries
type btree struct {
	root  *btreeNode
	rid   uint32
	len   int
	nodes []*btreeNode
}

// reset clears the btree back to an empty state
func (b *btree) reset() {
	*b = btree{}
}

// search returns the leaf btreeNode that should contain the key.
func (b *btree) search(key, buf []byte) (*btreeNode, uint32) {
	var prefixBytes [4]byte
	copy(prefixBytes[:], key)
	prefix := binary.BigEndian.Uint32(prefixBytes[:])

	n, nid := b.root, b.rid

	for !n.leaf {
		// binary search to find the appropriate child
		i, j := uint8(0), n.count
		for i < j {
			h := (i + j) >> 1
			enth := n.payload[h]
			prefixh := binary.BigEndian.Uint32(enth.prefix[:])

			// first, check the saved prefix. this avoids having to hop and
			// read the key if one is different from the other.
			switch compare(prefix, prefixh) {
			case 1:
				i = h + 1
			case 0:
				kh := enth.readKey(buf)
				if bytes.Compare(key, kh) >= 0 {
					i = h + 1
				} else {
					j = h
				}
			case -1:
				j = h
			}
		}

		if i == n.count {
			nid = n.next
		} else {
			nid = n.payload[i].pivot
		}
		n = b.nodes[nid]
	}

	return n, nid
}

// alloc creates a fresh btreeNode.
func (b *btree) alloc(leaf bool) (*btreeNode, uint32) {
	n := new(btreeNode)
	n.leaf = leaf
	n.next = invalidNode
	n.prev = invalidNode
	n.parent = invalidNode
	b.nodes = append(b.nodes, n)
	return n, uint32(len(b.nodes) - 1)
}

var splitThunk mon.Thunk

// split the node in half, returning a new node containing the
// smaller half of the keys.
func (b *btree) split(n *btreeNode, nid uint32) (*btreeNode, uint32) {
	timer := splitThunk.Start()

	s, sid := b.alloc(n.leaf)
	s.parent = n.parent

	// split the entries between the two nodes
	s.count = uint8(copy(s.payload[:], n.payload[:payloadSplit]))

	copyAt := payloadSplit
	if !n.leaf {
		// if it's not a leaf, we don't want to include the split entry
		copyAt++

		// additionally, the next pointer should be what the split entry
		// points at.
		s.next = n.payload[payloadSplit].pivot

		// additionally, every element that it points at needs to have
		// their parent updated
		b.nodes[s.next].parent = sid
		for i := uint8(0); i < s.count; i++ {
			b.nodes[s.payload[i].pivot].parent = sid
		}
	} else {
		// if it is a leaf, fix up the next and previous pointers
		s.next = nid
		if n.prev != invalidNode {
			s.prev = n.prev
			b.nodes[s.prev].next = sid
		}
		n.prev = sid
	}
	n.count = uint8(copy(n.payload[:], n.payload[copyAt:]))

	timer.Stop()
	return s, sid
}

// Insert puts the entry into the btree, using the buf to read keys
// to determine the position.
func (b *btree) Insert(ent entry, buf []byte) {
	key := ent.readKey(buf)

	// easy case: if we have no root, we can just allocate it
	// and insert the entry.
	if b.root == nil {
		b.root, b.rid = b.alloc(true)
		b.root.insertEntry(key, ent, buf)
		b.len++
		return
	}

	// search for the leaf that should contain the node
	n, nid := b.search(key, buf)
	for {
		if n.insertEntry(key, ent, buf) && n.leaf {
			b.len++
		}

		// easy case: if the node still has enough room, we're done.
		if n.count < payloadEntries {
			return
		}

		// update the entry we're going to insert to be the entry we're
		// splitting the node on.
		ent = n.payload[payloadSplit]

		// split the node. s is a new node that contains keys
		// smaller than the splitEntry.
		s, sid := b.split(n, nid)

		// find the parent, allocating a new node if we're looking
		// at the root, and set the parent of the split node.
		var p *btreeNode
		var pid uint32
		if n.parent != invalidNode {
			p, pid = b.nodes[n.parent], n.parent
		} else {
			// create a new parent node, and make it point at the
			// larger side of the split node for it's next pointer.
			p, pid = b.alloc(false)
			p.next = nid
			n.parent = pid
			s.parent = pid

			// store it as the root
			b.root, b.rid = p, pid
		}

		// make a pointer out of the split entry to point at the
		// newly split node, and try to insert it.
		ent.pivot = sid
		n, nid = p, pid
	}
}

// append adds the entry to the node, splitting if necessary. the entry must
// be greater than any entry already in the node. n remains to the right of
// and at least as low than any newly created nodes.
func (b *btree) append(n *btreeNode, nid uint32, ent entry) {
	for {
		n.appendEntry(ent)
		b.len++

		// easy case: if the node still has enough room, we're done.
		if n.count < payloadEntries {
			return
		}

		// update the entry we're going to insert to be the entry we're
		// splitting the node on.
		ent = n.payload[payloadSplit]

		// split the node. s is a new node that contains keys
		// smaller than the splitEntry.
		s, sid := b.split(n, nid)

		// find the parent, allocating a new node if we're looking
		// at the root, and set the parent of the split node.
		var p *btreeNode
		var pid uint32
		if n.parent != invalidNode {
			p, pid = b.nodes[n.parent], n.parent
		} else {
			// create a new parent node, and make it point at the
			// larger side of the split node for it's next pointer.
			p, pid = b.alloc(false)
			p.next = nid
			n.parent = pid
			s.parent = pid

			// store it as the root
			b.root, b.rid = p, pid
		}

		// make a pointer out of the split entry to point at the
		// newly split node, and try to insert it.
		ent.pivot = sid
		n, nid = p, pid
	}
}

// Iter calls the callback with all of the entries in order.
func (b *btree) Iter(cb func(ent *entry) bool) {
	n := b.root
	if b.root == nil {
		return
	}

	for !n.leaf {
		nid := n.payload[0].pivot
		if n.count == 0 {
			nid = n.next
		}
		n = b.nodes[nid]
	}

	for {
		for i := uint8(0); i < n.count; i++ {
			if !cb(&n.payload[i]) {
				return
			}
		}
		if n.next == invalidNode {
			return
		}
		n = b.nodes[n.next]
	}
}
