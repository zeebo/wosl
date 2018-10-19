package node

import (
	"bytes"
	"encoding/binary"

	"github.com/zeebo/errs"
	"github.com/zeebo/wosl/internal/mon"
)

// Error is the class that contains all the errors from this package.
var Error = errs.Class("node")

const nodeHeaderSize = (0 +
	4 + // capacity
	4 + // next
	4 + // count
	2 + // height
	0)

// T is a node in a write-optimized skip list. It targets a specific size
// and maintains entry pointers into the buf.
type T struct {
	buf      []byte // contains metadata and then key/value data
	capacity int32  // capacity of the node
	next     uint32 // pointer to the next node
	height   int16  // the height of the node
	entries  []entry
}

// New returns a node with a buffer size of the given size.
func New(capacity int32, next uint32, height int16) *T {
	return &T{
		buf:      make([]byte, nodeHeaderSize, capacity),
		capacity: capacity,
		next:     next,
		height:   height,
	}
}

var loadThunk mon.Thunk // timing info for Load

// Load reloads a node from the given buffer.
func Load(buf []byte) (*T, error) {
	defer loadThunk.Start().Stop()

	if len(buf) < nodeHeaderSize {
		return nil, Error.New("buffer too small: %d", buf)
	}

	// read in the header
	var (
		capacity = int32(binary.BigEndian.Uint32(buf[0:4]))
		next     = uint32(binary.BigEndian.Uint32(buf[4:8]))
		count    = int32(binary.BigEndian.Uint32(buf[8:12]))
		height   = int16(binary.BigEndian.Uint16(buf[12:14]))
	)

	if int32(len(buf)) != capacity {
		return nil, Error.New("buffer capacity does not match length: %d != %d",
			capacity, len(buf))
	}
	if count < 0 {
		return nil, Error.New("invalid count: %d", count)
	}

	// read in the entries
	offset := int64(nodeHeaderSize)
	entries := make([]entry, 0, count)
	b := buf[nodeHeaderSize:]

	for i := int32(0); i < count; i++ {
		ent, ok := readEntry(offset, b)
		if !ok {
			return nil, Error.New("buffer did not contain enough entries: %d != %d",
				i, count)
		}

		size := ent.size()
		if int64(len(b)) < size {
			return nil, Error.New("entry key/value data overran buffer")
		}

		offset += size
		b = b[size:]
		entries = append(entries, ent)
	}

	return &T{
		buf:      buf,
		capacity: capacity,
		next:     next,
		height:   height,
		entries:  entries,
	}, nil
}

// Capacity returns the number of bytes of capacity the node has.
func (t *T) Capacity() int32 { return t.capacity }

// Height returns the height of the node.
func (t *T) Height() int16 { return t.height }

// Next returns the next node pointer. It will have the same height.
func (t *T) Next() uint32 { return t.next }

// LeaderPivot returns the pivot of the entry that is the leader.
func (t *T) LeaderPivot() uint32 {
	if len(t.entries) == 0 {
		return 0
	}
	return t.entries[0].pivot
}

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
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(t.entries)))
	binary.BigEndian.PutUint16(buf[12:14], uint16(t.height))

	buf = buf[:nodeHeaderSize]
	for _, ent := range t.entries {
		hdr := ent.header()
		buf = append(buf, hdr[:]...)
		buf = append(buf, ent.readKey(t.buf)...)
		buf = append(buf, ent.readValue(t.buf)...)
	}

	return nil
}

// Reset returns the node to the initial new state.
func (t *T) Reset() {
	t.buf = t.buf[:nodeHeaderSize]
	t.entries = t.entries[:0]
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
		kindOff: kindInsert | int32(len(t.buf))<<8,
		key:     int32(len(key)),
		value:   int32(len(value)),
	}

	// TODO(jeff): we can check to see if the entry already exists
	// first, and then if the new value is smaller than the old one
	// we can just overwrite it directly in the buffer. would have
	// to remember to update the kindOff value. this could help out
	// with reducing the number of flushes required for nodes that
	// often see the same keys and values that don't increase.

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
	t.insertEntry(key, ent)

	timer.Stop()
	return true
}

var insertSentinelThunk mon.Thunk // timing info for InsertSentinel

// InsertSentinel adds the sentinel entry to the node.
func (t *T) InsertSentinel(pivot uint32) {
	timer := insertSentinelThunk.Start()

	// TODO(jeff): should this be a flag on New?

	ent := entry{
		pivot:   pivot,
		kindOff: kindSentinel | int32(len(t.buf))<<8,
		key:     0,
		value:   0,
	}

	hdr := ent.header()
	t.buf = append(t.buf, hdr[:]...)

	t.entries = append(t.entries, entry{})
	copy(t.entries[1:], t.entries)
	t.entries[0] = ent

	timer.Stop()
}

var deleteThunk mon.Thunk // timing info for Delete

// Delete removes the key from the node. It does not reclaim space
// in the buffer. If wrote is false, there was not enough space, and
// the node should be flushed.
func (t *T) Delete(key []byte) (wrote bool) {
	timer := deleteThunk.Start()

	ent := entry{
		pivot:   0,
		kindOff: kindTombstone | int32(len(t.buf))<<8,
		key:     int32(len(key)),
		value:   0,
	}

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
	t.insertEntry(key, ent)

	timer.Stop()
	return true
}

var insertEntryThunk mon.Thunk // timing info for insertEntry

// insertEntry binary searches the slice of entries and does
// an insertion in the correct spot. Maybe there's a better
// way to keep a sorted list, but this is what I've got.
func (t *T) insertEntry(key []byte, ent entry) {
	timer := insertEntryThunk.Start()

	// TODO(jeff): Should we do special case checks for if the entry is either
	// before every entry or after every entry? Those are O(1) and would maybe
	// improve the sorted/reverse-sorted insert cases.

	i, j := 0, len(t.entries)

	for i < j {
		h := int(uint(i+j) >> 1)
		enth := t.entries[h]

		if byte(enth.kindOff) == kindSentinel || // is this the -infinity node?
			bytes.Compare(enth.readKey(t.buf), key) == -1 {

			i = h + 1
		} else {
			j = h
		}
	}

	if i >= len(t.entries) {
		t.entries = append(t.entries, ent)
	} else if bytes.Equal(t.entries[i].readKey(t.buf), key) {
		t.entries[i] = ent
	} else {
		t.entries = append(t.entries, entry{})
		copy(t.entries[i+1:], t.entries[i:])
		t.entries[i] = ent
	}

	timer.Stop()
}
