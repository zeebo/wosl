package node

import (
	"bytes"
	"encoding/binary"
	"math"
	"unsafe"
)

const (
	invalidNode    = math.MaxUint32
	payloadEntries = 127
	payloadSplit   = payloadEntries / 2
	btreeNodeSize  = uint64(unsafe.Sizeof(btreeNode{}))
)

// N.B. it is important that btreeNode does not contain pointers, so that we can
// construct arrays of them off heap.

// btreeNode are nodes in the btree.
type btreeNode struct {
	next    uint32  // pointer to the next node (or if not leaf, the rightmost edge)
	prev    uint32  // backpointer from next node (unused if not leaf)
	parent  uint32  // set to invalidNode on the root node
	count   uint16  // used values in payload
	leaf    bool    // set if is a leaf
	_       [1]byte // padding
	payload [payloadEntries]Entry
}

// insertEntry inserts the entry into the node. it should never be called
// on a node that would have to split. it returns true if the count increased.
func (n *btreeNode) insertEntry(key []byte, ent Entry, buf []byte) bool {
	prefix := binary.BigEndian.Uint32(ent.prefix[:])

	// binary search to find the appropriate child
	i, j := uint16(0), n.count
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

			case 0:
				// found a match. overwite and exit.
				// we want to retain the pivot field, though.
				ent.pivot = enth.pivot
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
func (n *btreeNode) appendEntry(ent Entry) {
	n.payload[n.count] = ent
	n.count++
}
