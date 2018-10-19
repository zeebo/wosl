package node

import (
	"bytes"
	"encoding/binary"

	"github.com/zeebo/errs"
	"github.com/zeebo/wosl/internal/mon"
)

// Error is the class that contains all the errors from this package.
var Error = errs.Class("node")

const (
	// the different kinds of entries. kindSentinel is used for the special
	// element that ensures there is a root for the entire structure.
	kindInsert = iota
	kindTombstone
	kindSentinel
)

const entryHeaderSize = (0 +
	4 + // pivot
	1 + // kind
	4 + // key length
	4 + // value length
	0)

// TODO(jeff): entry insertion will go faster the smaller this struct is.
// maybe we can do more aggressive packing at the cost of making readKey
// more expensive. pivot, kind, and value are all not used during inserts.

// entry is kept in sorted order in a node's memory buffer.
type entry struct {
	pivot   uint32 // 0 means no pivot: there is no block 0.
	kindOff int32  // kind is lower 8 bits of kindOff
	key     int32
	value   int32
}

// size returns how many bytes the entry consumes
func (e entry) size() int64 { return int64(entryHeaderSize) + int64(e.key) + int64(e.value) }

// header returns an array of bytes containing the entry header.
func (e entry) header() (hdr [entryHeaderSize]byte) {
	binary.BigEndian.PutUint32(hdr[0:4], uint32(e.pivot))
	hdr[4] = byte(e.kindOff)
	binary.BigEndian.PutUint32(hdr[5:9], uint32(e.key))
	binary.BigEndian.PutUint32(hdr[9:13], uint32(e.value))
	return hdr
}

// readKey returns a slice of the buffer that contains the key.
func (e entry) readKey(buf []byte) []byte {
	start := entryHeaderSize + e.kindOff>>8
	return buf[start : start+e.key]
}

// readValue returns a slice of the buffer that contains the value.
func (e entry) readValue(buf []byte) []byte {
	start := entryHeaderSize + e.kindOff>>8 + e.key
	return buf[start : start+e.value]
}

// readEntry returns an entry from the beginning of the buf given that
// the buf is offset bytes in.
func readEntry(offset int64, buf []byte) (entry, bool) {
	if len(buf) < entryHeaderSize {
		return entry{}, false
	}
	return entry{
		pivot:   uint32(binary.BigEndian.Uint32(buf[0:4])),
		kindOff: int32(buf[4]) | int32(offset)<<8,
		key:     int32(binary.BigEndian.Uint32(buf[5:9])),
		value:   int32(binary.BigEndian.Uint32(buf[9:13])),
	}, true
}

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

var loadThunk mon.Thunk

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

var writeThunk mon.Thunk

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

// reset returns the node to the initial new state.
func (t *T) reset() {
	t.buf = t.buf[:32]
	t.entries = t.entries[:0]
}

var insertThunk mon.Thunk

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

var deleteThunk mon.Thunk

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

var insertEntryThunk mon.Thunk

// insertEntry binary searches the slice of entries and does
// an insertion in the correct spot. Maybe there's a better
// way to keep a sorted list, but this is what I've got.
func (t *T) insertEntry(key []byte, ent entry) {
	timer := insertEntryThunk.Start()

	i, j := 0, len(t.entries)

	for i < j {
		h := int(uint(i+j) >> 1)
		hkey := t.entries[h].readKey(t.buf)

		if bytes.Compare(hkey, key) == -1 {
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
