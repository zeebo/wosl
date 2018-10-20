package node

import (
	"encoding/binary"
	"math"

	"github.com/zeebo/errs"
	"github.com/zeebo/wosl/internal/mon"
)

// Error is the class that contains all the errors from this package.
var Error = errs.Class("node")

const nodeHeaderSize = (0 +
	4 + // capacity
	4 + // next
	4 + // pivot
	4 + // count
	0)

// T is a node in a write-optimized skip list. It targets a specific size
// and maintains entry pointers into the buf.
type T struct {
	buf      []byte // contains metadata and then key/value data
	capacity int32  // capacity of the node
	next     uint32 // pointer to the next node (or 0)
	pivot    uint32 // pivot for special root node
	entries  btree  // btree of entries into buf
}

// New returns a node with a buffer size of the given size.
func New(capacity int32, next uint32) *T {
	return &T{
		buf:      make([]byte, nodeHeaderSize, capacity),
		capacity: capacity,
		next:     next,
	}
}

var loadThunk mon.Thunk // timing info for Load

// Load reloads a node from the given buffer.
func Load(buf []byte) (*T, error) {
	timer := loadThunk.Start()

	// TODO(jeff): Consider only doing O(1) loading of the buffer
	// and doing a double index into the buffer in order to read
	// entries, rather than keeping an entries slice. this may
	// complicate the insert path quite a bit, though.

	if len(buf) < nodeHeaderSize {
		timer.Stop()
		return nil, Error.New("buffer too small: %d", buf)
	}

	// read in the header
	var (
		capacity = int32(binary.BigEndian.Uint32(buf[0:4]))
		next     = uint32(binary.BigEndian.Uint32(buf[4:8]))
		pivot    = uint32(binary.BigEndian.Uint32(buf[8:12]))
		count    = int32(binary.BigEndian.Uint32(buf[12:16]))
	)

	if int32(len(buf)) != capacity {
		timer.Stop()
		return nil, Error.New("buffer capacity does not match length: %d != %d",
			capacity, len(buf))
	}
	if count < 0 {
		timer.Stop()
		return nil, Error.New("invalid count: %d", count)
	}

	// read in the entries
	bulk := new(btreeBulk)
	offset := uint32(nodeHeaderSize)
	b := buf[nodeHeaderSize:]

	for i := int32(0); i < count; i++ {
		ent, ok := readEntry(offset, b)
		if !ok {
			timer.Stop()
			return nil, Error.New("buffer did not contain enough entries: %d != %d",
				i, count)
		}

		size := ent.size()
		if int64(len(b)) < size || size > math.MaxUint32 {
			timer.Stop()
			return nil, Error.New("entry key/value data overran buffer")
		}

		offset += uint32(size)
		b = b[size:]

		bulk.append(ent)
	}

	timer.Stop()
	return &T{
		buf:      buf,
		capacity: capacity,
		next:     next,
		pivot:    pivot,
		entries:  bulk.done(),
	}, nil
}

// Capacity returns the number of bytes of capacity the node has.
func (t *T) Capacity() int32 { return t.capacity }

// Next returns the next node pointer.
func (t *T) Next() uint32 { return t.next }

// SetNext sets the next pointer.
func (t *T) SetNext(next uint32) { t.next = next }

// Pivot returns the pivot node pointer for the root nodes.
func (t *T) Pivot() uint32 { return t.pivot }

// SetPivot sets the next pointer.
func (t *T) SetPivot(pivot uint32) { t.pivot = pivot }

var writeThunk mon.Thunk // timing info for Write

// Write marshals the node to the provided buffer. It must be the
// appropriate size.
func (t *T) Write(buf []byte) error {
	defer writeThunk.Start().Stop()

	if int32(len(buf)) != t.capacity || len(buf) < nodeHeaderSize {
		return Error.New("invalid buffer capacity: %d != %d", len(buf), t.capacity)
	}

	binary.BigEndian.PutUint32(buf[0:4], uint32(t.capacity))
	binary.BigEndian.PutUint32(buf[4:8], uint32(t.next))
	binary.BigEndian.PutUint32(buf[8:12], uint32(t.pivot))
	binary.BigEndian.PutUint32(buf[12:16], uint32(t.entries.len))

	buf = buf[:nodeHeaderSize]
	t.entries.Iter(func(ent entry) bool {
		hdr := ent.header()
		buf = append(buf, hdr[:]...)
		buf = append(buf, ent.readKey(t.buf)...)
		buf = append(buf, ent.readValue(t.buf)...)
		return true
	})

	return nil
}

// Reset returns the node to the initial new state.
func (t *T) Reset() {
	t.entries.reset()
	t.buf = t.buf[:nodeHeaderSize]
}

var insertThunk mon.Thunk // timing info for Insert

// Insert associates the key with the value in the node. If wrote is
// false, then there was not enough space, and the node should be
// flushed.
func (t *T) Insert(key, value []byte) (wrote bool) {
	timer := insertThunk.Start()

	// build the entry that we will insert.
	ent := entry{
		pivot:   0,
		key:     uint32(len(key)),
		value:   uint32(len(value)),
		kindOff: kindInsert | uint32(len(t.buf))<<8,
	}

	// TODO(jeff): we can check to see if the entry already exists
	// first, and then if the new value is smaller than the old one
	// we can just overwrite it directly in the buffer. would have
	// to remember to update the kindOff value. this could help out
	// with reducing the number of flushes required for nodes that
	// often see the same keys and values that don't increase.
	// doing the insert into the b+tree already requires doing a
	// search, anyway, so we should be able to piggyback on it.

	// if we don't have the size to insert it, return false
	// and wait for someone to flush us.
	if int64(len(t.buf))+int64(ent.size()) > int64(t.capacity) {
		timer.Stop()
		return false
	}

	// add the entry to the buffer
	hdr := ent.header()
	t.buf = append(t.buf, hdr[:]...)
	t.buf = append(t.buf, key...)
	t.buf = append(t.buf, value...)

	// insert it into the slice
	t.entries.Insert(ent, t.buf)

	timer.Stop()
	return true
}

var deleteThunk mon.Thunk // timing info for Delete

// Delete removes the key from the node. It does not reclaim space
// in the buffer. If wrote is false, there was not enough space, and
// the node should be flushed.
func (t *T) Delete(key []byte) (wrote bool) {
	timer := deleteThunk.Start()

	ent := entry{
		pivot:   0,
		key:     uint32(len(key)),
		value:   0,
		kindOff: kindTombstone | uint32(len(t.buf))<<8,
	}

	// TODO(jeff): see comment about searching first in insert.

	// if we don't have the size to insert it, return false
	// and wait for someone to flush us.
	if int64(len(t.buf))+int64(ent.size()) > int64(t.capacity) {
		timer.Stop()
		return false
	}

	// add the entry to the buffer
	hdr := ent.header()
	t.buf = append(t.buf, hdr[:]...)
	t.buf = append(t.buf, key...)

	// insert it into the slice
	t.entries.Insert(ent, t.buf)

	timer.Stop()
	return true
}
