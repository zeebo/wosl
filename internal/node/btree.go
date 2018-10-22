package node

import (
	"bytes"
	"encoding/binary"
	"unsafe"

	"github.com/zeebo/wosl/internal/mon"
)

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
func (b *btree) Iter(cb func(ent entry) bool) {
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
			if !cb(n.payload[i]) {
				return
			}
		}
		if n.next == invalidNode {
			return
		}
		n = b.nodes[n.next]
	}
}

// size returns how many bytes writing out the btree would take
func (b *btree) size() int { return btreeNodeSize * len(b.nodes) }

// write uses the storage in buf to write the btree if possible. if not
// possible, it allocates a new slice.
func (b *btree) write(buf []byte) []byte {
	if size := b.size(); cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}

	w := buf
	for _, n := range b.nodes {
		// TODO(jeff): be explicit with encoding/binary and check the perf
		// difference. probably not much.
		*(*btreeNode)(unsafe.Pointer(&w[0])) = *n
		w = w[btreeNodeSize:]
	}

	return buf
}
