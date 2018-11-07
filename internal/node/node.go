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
	4 + // next
	4 + // height
	4 + // pivot
	8 + // btree size
	0)

// how many bytes a node header is when padded
const nodeHeaderPadded = btreeNodeSize - btreeHeaderSize

// TODO(jeff): investigate using a [][]byte (or just two []byte) so that we
// don't have to copy potentially large amounts of data to just append a new
// key. it may be mmap'd causing a bunch of read traffic for no reason. the
// buf is append only, anyway, so there has to be some way to optimize.
// maybe it'll only be slower for typical values of buf (like 4MB or less).

// T is a node in a write-optimized skip list. It targets a specific size
// and maintains entry pointers into the buf.
type T struct {
	next    uint32 // pointer to the next node (or 0)
	height  uint32 // height of the node
	pivot   uint32 // pivot for special root node
	buf     []byte // buffer containing the keys and values
	base    uint32 // how many bytes into buf the key/values start
	entries btree  // btree of entries into buf
	dirty   bool   // if modifications have happened since the last Write
}

// New returns a node with a buffer size of the given size.
func New(next, height uint32) *T {
	return &T{
		next:   next,
		height: height,
	}
}

var nodeLoadThunk mon.Thunk // timing info for node.Load

// Load returns a node from reading the given buffer.
func Load(buf []byte) (*T, error) {
	timer := nodeLoadThunk.Start()

	if len(buf) < nodeHeaderSize {
		timer.Stop()
		return nil, Error.New("buffer too small: %d", buf)
	}

	// read in the header
	var (
		next      = uint32(binary.BigEndian.Uint32(buf[0:4]))
		height    = uint32(binary.BigEndian.Uint32(buf[4:8]))
		pivot     = uint32(binary.BigEndian.Uint32(buf[8:12]))
		btreeSize = uint64(binary.BigEndian.Uint64(buf[12:20]))
	)

	if uint64(len(buf)) < nodeHeaderPadded+btreeSize {
		timer.Stop()
		return nil, Error.New("buffer too small: %d", len(buf))
	}

	entries, err := loadBtree(buf[nodeHeaderPadded:])
	if err != nil {
		timer.Stop()
		return nil, Error.Wrap(err)
	}

	base := nodeHeaderPadded + entries.Length()
	if base > math.MaxUint32 {
		timer.Stop()
		return nil, Error.New("internal error: btree too big")
	}

	timer.Stop()
	return &T{
		buf:     buf,
		next:    next,
		height:  height,
		pivot:   pivot,
		base:    uint32(base),
		entries: entries,
	}, nil
}

// Length returns an upper bound on how many bytes writing the node would require.
func (t *T) Length() uint64 {
	return 0 +
		nodeHeaderPadded +
		t.entries.Length() +
		(uint64(len(t.buf)) - uint64(t.base)) +
		0
}

// Count returns how many entries are in the node.
func (t *T) Count() uint32 { return uint32(t.entries.entries) }

// Height returns the height of the node.
func (t *T) Height() uint32 { return t.height }

// Next returns the next node pointer.
func (t *T) Next() uint32 { return t.next }

// SetNext sets the next pointer.
func (t *T) SetNext(next uint32) { t.next = next }

// Pivot returns the pivot node pointer for the root nodes.
func (t *T) Pivot() uint32 { return t.pivot }

// SetPivot sets the next pointer.
func (t *T) SetPivot(pivot uint32) { t.pivot = pivot }

// Dirty returns true if the node has been modified since the last Write.
func (t *T) Dirty() bool { return t.dirty }

// Sully forces the node to be dirty, even if no writes have happened.
func (t *T) Sully() { t.dirty = true }

var nodeWriteThunk mon.Thunk // timing info for node.Write

// TODO(jeff): do we want to include the lengths before the key and value so
// that a scan doesn't have to continue to load what the next entry is from
// the btree? it'd be 4 bytes per entry (since we have keys are 10 bits and
// values are 20 bits), but maybe the btree hopping isn't expensive?

// Write marshals the node to the provided buffer. If it is not large enough
// a new one is allocated. It holds on to the returned buffer, so do not
// modify it.
func (t *T) Write(buf []byte) ([]byte, error) {
	timer := nodeWriteThunk.Start()

	length := t.Length()
	if length > math.MaxUint32 {
		return nil, Error.New("internal error: node has become too big")
	}

	// ensure buf is large enough
	if uint64(cap(buf)) < uint64(length) {
		buf = make([]byte, length)
	} else {
		buf = buf[:length]
	}

	btreeSize := t.entries.Length()

	// write in the header
	binary.BigEndian.PutUint32(buf[0:4], uint32(t.next))
	binary.BigEndian.PutUint32(buf[4:8], uint32(t.height))
	binary.BigEndian.PutUint32(buf[8:12], uint32(t.pivot))
	binary.BigEndian.PutUint64(buf[12:20], uint64(btreeSize))

	// compact the entries so that their offsets are increasing
	data := buf[nodeHeaderPadded+btreeSize : nodeHeaderPadded+btreeSize : len(buf)]
	t.entries.Iter(func(ent *Entry) bool {
		offset := uint32(len(data))
		data = append(data, ent.readEntry(buf)...)
		ent.offset = offset
		return true
	})

	// write in the compacted btree
	t.entries.write(buf[nodeHeaderPadded:])

	// update our local state because we modified the btree entries
	t.buf = buf
	t.base = uint32(nodeHeaderPadded + btreeSize)
	t.dirty = false

	timer.Stop()
	return buf, nil
}

// Reset returns the node to the initial new state, even if it was
// created from a call to Load.
func (t *T) Reset() {
	t.buf = t.buf[:0]
	t.base = 0
	t.entries.reset()
	t.dirty = false
}

// Fits returns if a write for the given key would fit in size.
func (t *T) Fits(key, value []byte, size uint32) bool {
	return len(key) <= keyMask &&
		len(value) <= valueMask &&
		// we add 10 btreeNodeSize to protect if the insert would cause a split
		// which might allocate up to log(n) nodes. there's no way that's ever
		// bigger than 10 (famous last words).
		t.Length()+10*btreeNodeSize < uint64(size)
}

var nodeInsertThunk mon.Thunk // timing info for node.Insert

// Insert associates the key with the value in the node. If wrote is
// false, then there was not enough space, and the node should be
// flushed.
func (t *T) Insert(key, value []byte) (wrote bool) {
	timer := nodeInsertThunk.Start()

	// make sure the write is ok to go
	if !t.Fits(key, value, math.MaxUint32) {
		timer.Stop()
		return false
	}

	// build the entry that we will insert.
	ent := newEntry(key, value, false, uint32(len(t.buf))-t.base)

	// add the data to the buffer
	t.buf = append(t.buf, key...)
	t.buf = append(t.buf, value...)

	// insert it into the btree.
	t.entries.Insert(ent, t.buf)
	t.dirty = true

	timer.Stop()
	return true
}

var nodeDeleteThunk mon.Thunk // timing info for node.Delete

// Delete removes the key from the node. It does not reclaim space
// in the buffer. If wrote is false, there was not enough space, and
// the node should be flushed.
func (t *T) Delete(key []byte) (wrote bool) {
	timer := nodeDeleteThunk.Start()

	// make sure the write is ok to go
	if !t.Fits(key, nil, math.MaxUint32) {
		timer.Stop()
		return false
	}

	// build the entry that we will insert.
	ent := newEntry(key, nil, true, uint32(len(t.buf))-t.base)

	// add the data to the buffer
	t.buf = append(t.buf, key...)

	// insert it into the btree
	t.entries.Insert(ent, t.buf)
	t.dirty = true

	timer.Stop()
	return true
}
