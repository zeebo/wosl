package btree

import (
	"bytes"
	"encoding/binary"
	"unsafe"

	"github.com/zeebo/errs"
	"github.com/zeebo/mon"
	"github.com/zeebo/wosl/internal/node/entry"
)

// Error is the class that contains all the errors from this package.
var Error = errs.Class("btree")

// btree is an in memory B+ tree tuned to store entries
type T struct {
	root  *node
	rid   uint32
	count uint32
	nodes []*node
}

// Reset clears the btree back to an empty state
func (b *T) Reset() {
	*b = T{}
}

// search returns the leaf node that should contain the key.
func (b *T) search(key, buf []byte) (*node, uint32) {
	var prefixBytes [4]byte
	copy(prefixBytes[:], key)
	prefix := binary.BigEndian.Uint32(prefixBytes[:])

	n, nid := b.root, b.rid

	for !n.leaf {
		// binary search to find the appropriate child
		i, j := uint16(0), n.count
		for i < j {
			h := (i + j) >> 1
			enth := n.payload[h]
			prefixh := binary.BigEndian.Uint32(enth.Prefix[:])

			// first, check the saved prefix. this avoids having to hop and
			// read the key if one is different from the other.
			switch compare(prefix, prefixh) {
			case 1:
				i = h + 1
			case 0:
				kh := enth.ReadKey(buf)
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
			nid = n.payload[i].Pivot()
		}
		n = b.nodes[nid]
	}

	return n, nid
}

// alloc creates a fresh node.
func (b *T) alloc(leaf bool) (*node, uint32) {
	n := new(node)
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
func (b *T) split(n *node, nid uint32) (*node, uint32) {
	timer := splitThunk.Start()

	s, sid := b.alloc(n.leaf)
	s.parent = n.parent

	// split the entries between the two nodes
	s.count = uint16(copy(s.payload[:], n.payload[:payloadSplit]))

	copyAt := payloadSplit
	if !n.leaf {
		// if it's not a leaf, we don't want to include the split entry
		copyAt++

		// additionally, the next pointer should be what the split entry
		// points at.
		s.next = n.payload[payloadSplit].Pivot()

		// additionally, every element that it points at needs to have
		// their parent updated
		b.nodes[s.next].parent = sid
		for i := uint16(0); i < s.count; i++ {
			b.nodes[s.payload[i].Pivot()].parent = sid
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
	n.count = uint16(copy(n.payload[:], n.payload[copyAt:]))

	timer.Stop()
	return s, sid
}

// Insert puts the entry into the btree, using the buf to read keys
// to determine the position. It returns true if the insert created
// a new entry.
func (b *T) Insert(ent entry.T, buf []byte) bool {
	key := ent.ReadKey(buf)

	// easy case: if we have no root, we can just allocate it
	// and insert the entry.
	if b.root == nil {
		b.root, b.rid = b.alloc(true)
		b.root.insertEntry(key, ent, buf)
		b.count++
		return true
	}

	// search for the leaf that should contain the node
	n, nid := b.search(key, buf)
	for {
		added := n.insertEntry(key, ent, buf)
		if added && n.leaf {
			b.count++
		}

		// easy case: if the node still has enough room, we're done.
		if n.count < payloadEntries {
			return added
		}

		// update the entry we're going to insert to be the entry we're
		// splitting the node on.
		ent = n.payload[payloadSplit]

		// split the node. s is a new node that contains keys
		// smaller than the splitEntry.
		s, sid := b.split(n, nid)

		// find the parent, allocating a new node if we're looking
		// at the root, and set the parent of the split node.
		var p *node
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
		ent.SetPivot(sid)
		n, nid = p, pid
	}
}

// append adds the entry to the node, splitting if necessary. the entry must
// be greater than any entry already in the node. n remains to the right of
// and at least as low than any newly created nodes.
func (b *T) append(n *node, nid uint32, ent entry.T) {
	for {
		n.appendEntry(ent)
		b.count++

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
		var p *node
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
		ent.SetPivot(sid)
		n, nid = p, pid
	}
}

// Iter calls the callback with all of the entries in order.
func (b *T) Iter(cb func(ent *entry.T) bool) {
	n := b.root
	if b.root == nil {
		return
	}

	for !n.leaf {
		nid := n.payload[0].Pivot()
		if n.count == 0 {
			nid = n.next
		}
		n = b.nodes[nid]
	}

	for {
		for i := uint16(0); i < n.count; i++ {
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

func (b *T) Iterator() Iterator {
	// find the deepest leftmost node
	n := b.root
	if n == nil {
		return Iterator{}
	}

	for !n.leaf {
		nid := n.payload[0].Pivot()
		if n.count == 0 {
			nid = n.next
		}
		n = b.nodes[nid]
	}

	return Iterator{
		b: b,
		n: n,
		i: uint16(1<<16 - 1), // overflow hack. this is -1
	}
}

// HeaderSize is the number of bytes the btree header takes up
const HeaderSize = 0 +
	4 + // root id
	4 + // number of entries
	4 + // number of nodes
	0

// Length returns how many bytes writing out the btree would take
func (b *T) Length() uint64 { return HeaderSize + NodeSize*uint64(len(b.nodes)) }

// Count returns how many entries are in the btree.
func (b *T) Count() uint32 { return b.count }

// Write uses the storage in buf to write the btree if possible. if not
// possible, it allocates a new slice.
func (b *T) Write(buf []byte) []byte {
	length := b.Length()
	if uint64(cap(buf)) < length {
		buf = make([]byte, length)
	} else {
		buf = buf[:length]
	}

	binary.LittleEndian.PutUint32(buf[0:4], b.rid)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(b.count))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(b.nodes)))

	w := buf[HeaderSize:]
	for _, n := range b.nodes {
		// TODO(jeff): check how expensive encoding/binary is.
		*(*node)(unsafe.Pointer(&w[0])) = *n
		w = w[NodeSize:]
	}

	return buf
}

// Load loads up a btree from the provided buffer. it continues to use
// the buffer as a backing store until it must grow.
func Load(buf []byte) (T, error) {
	if len(buf) < HeaderSize {
		return T{}, Error.New("buffer too small for btree")
	}

	var (
		rid    = binary.LittleEndian.Uint32(buf[0:4])
		count  = binary.LittleEndian.Uint32(buf[4:8])
		ncount = binary.LittleEndian.Uint32(buf[8:12])
	)

	if uint32(rid) >= ncount {
		return T{}, Error.New("root id out of range. root:%d count:%d",
			rid, ncount)
	}
	if uint64(len(buf)) < HeaderSize+uint64(NodeSize)*uint64(ncount) {
		return T{}, Error.New("buffer too small for %d nodes: %d",
			ncount, len(buf))
	}

	r := buf[HeaderSize:]
	nodes := make([]*node, ncount)
	for i := range nodes {
		// TODO(jeff): check how expensive encoding/binary is.
		nodes[i] = (*node)(unsafe.Pointer(&r[0]))
		r = r[NodeSize:]
	}

	return T{
		root:  nodes[rid],
		rid:   rid,
		count: count,
		nodes: nodes,
	}, nil
}
