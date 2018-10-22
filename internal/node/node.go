package node

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/zeebo/errs"
	"github.com/zeebo/wosl/internal/mon"
)

// We do O(1) loading of the buffer with a table of offsets to the nth
// entry in the buffer, rather than going through the btree for reads.
// plan:
// - store count > 0 from load to signal we have that many double index.
// - allow appends as usual, and insert into the btree as usual.
// - make an iter method that merges in from double index and btree.
// - make Write and IterKeys use the iter method.
// - make buf immutable when it came from load so that it can be backed
//   by mmap instead of reading the whole thing in.
// - have length account for both buffers.

// Error is the class that contains all the errors from this package.
var Error = errs.Class("node")

const nodeHeaderSize = (0 +
	4 + // next
	4 + // height
	4 + // pivot
	4 + // count
	0)

// T is a node in a write-optimized skip list. It targets a specific size
// and maintains entry pointers into the buf.
type T struct {
	buf     []byte // buffer containing the keys and values
	next    uint32 // pointer to the next node (or 0)
	height  uint32 // height of the node
	pivot   uint32 // pivot for special root node
	entries btree  // btree of entries into buf
	dirty   bool   // if modifications have happened since the last Write
	count   uint32 // number of double index entries in cbuf
	cbuf    []byte // buffer from Load
}

// New returns a node with a buffer size of the given size.
func New(next, height uint32) *T {
	return &T{
		buf:    make([]byte, nodeHeaderSize),
		next:   next,
		height: height,
	}
}

var loadThunk mon.Thunk // timing info for Load

// Load returns a node from reading the given buffer.
func Load(buf []byte) (*T, error) {
	timer := loadThunk.Start()

	if len(buf) < nodeHeaderSize {
		timer.Stop()
		return nil, Error.New("buffer too small: %d", buf)
	}

	// read in the header
	var (
		next   = uint32(binary.BigEndian.Uint32(buf[0:4]))
		height = uint32(binary.BigEndian.Uint32(buf[4:8]))
		pivot  = uint32(binary.BigEndian.Uint32(buf[8:12]))
		count  = uint32(binary.BigEndian.Uint32(buf[12:16]))
	)

	timer.Stop()
	return &T{
		next:   next,
		height: height,
		pivot:  pivot,
		count:  count,
		cbuf:   buf,
	}, nil
}

// Length returns an upper bound on how many bytes writing the node would require.
func (t *T) Length() uint32 { return uint32(len(t.buf)+len(t.cbuf)) + 4*uint32(t.entries.len) }

// Count returns how many entries are in the node.
func (t *T) Count() uint32 { return uint32(t.entries.len) + t.count }

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

var writeThunk mon.Thunk // timing info for Write

// Write marshals the node to the provided buffer, appending to it.
func (t *T) Write(buf []byte) ([]byte, error) {
	timer := writeThunk.Start()

	if len := int64(t.Length()); int64(cap(buf)) < len {
		buf = make([]byte, 0, len)
	}

	count := t.Count()

	buf = buf[:nodeHeaderSize]
	binary.BigEndian.PutUint32(buf[0:4], uint32(t.next))
	binary.BigEndian.PutUint32(buf[4:8], uint32(t.height))
	binary.BigEndian.PutUint32(buf[8:12], uint32(t.pivot))
	binary.BigEndian.PutUint32(buf[12:16], count)

	// make space for the table
	offset := len(buf)
	buf = buf[:int64(len(buf))+int64(4*count)]

	err := t.iter(func(ent entry, ebuf []byte) bool {
		// write a table entry for where the entry lives
		start := uint32(len(buf))
		binary.BigEndian.PutUint32(buf[offset:], start)
		offset += 4

		// write the entry
		hdr := ent.header()
		buf = append(buf, hdr[:]...)
		buf = append(buf, ent.readKey(ebuf)...)
		buf = append(buf, ent.readValue(ebuf)...)
		return true
	})
	if err != nil {
		return nil, Error.Wrap(err)
	}

	t.dirty = false

	timer.Stop()
	return buf, nil
}

// Reset returns the node to the initial new state, even if it was
// created from a call to Load.
func (t *T) Reset() {
	// TODO(jeff): we have to clear cbuf in order to make the
	// Length call return the right results, which means we
	// cant use it to figure out which mmap region to unmap
	// later. Just pointing this out in case that ends up
	// being a leak/bug.

	t.buf = t.buf[:nodeHeaderSize]
	t.entries.reset()
	t.dirty = false
	t.count = 0
	t.cbuf = nil
}

// Fits returns if a write for the given key would fit in size.
func (t *T) Fits(key, value []byte, size uint32) bool {
	return len(key) <= keyMask &&
		len(value) <= valueMask &&
		entryHeaderSize+int64(len(key))+int64(len(value))+4 < int64(size) &&
		t.Count() < math.MaxUint32
}

var insertThunk mon.Thunk // timing info for Insert

// Insert associates the key with the value in the node. If wrote is
// false, then there was not enough space, and the node should be
// flushed.
func (t *T) Insert(key, value []byte) (wrote bool) {
	timer := insertThunk.Start()

	// make sure the write is ok to go
	if !t.Fits(key, value, math.MaxUint32) {
		timer.Stop()
		return false
	}

	// build the entry that we will insert.
	ent := newEntry(key, value, kindInsert, uint32(len(t.buf)))

	// add the entry to the buffer
	hdr := ent.header()
	t.buf = append(t.buf, hdr[:]...)
	t.buf = append(t.buf, key...)
	t.buf = append(t.buf, value...)

	// insert it into the btree.
	t.entries.Insert(ent, t.buf)
	t.dirty = true

	timer.Stop()
	return true
}

var deleteThunk mon.Thunk // timing info for Delete

// Delete removes the key from the node. It does not reclaim space
// in the buffer. If wrote is false, there was not enough space, and
// the node should be flushed.
func (t *T) Delete(key []byte) (wrote bool) {
	timer := deleteThunk.Start()

	// make sure the write is ok to go
	if !t.Fits(key, nil, math.MaxUint32) {
		timer.Stop()
		return false
	}

	// build the entry that we will insert.
	ent := newEntry(key, nil, kindTombstone, uint32(len(t.buf)))

	// add the entry to the buffer
	hdr := ent.header()
	t.buf = append(t.buf, hdr[:]...)
	t.buf = append(t.buf, key...)

	// insert it into the btree
	t.entries.Insert(ent, t.buf)
	t.dirty = true

	timer.Stop()
	return true
}

// iter does a merged iteration over cbuf and t.entries.
func (t *T) iter(cb func(ent entry, buf []byte) bool) error {
	var (
		offset uint32 = nodeHeaderSize // offset into cbuf
		i      uint32                  // index into count
		ckey   []byte                  // current key from cbuf
		cent   entry
		err    error
	)

	// merge cbuf along with the btree
	t.entries.Iter(func(ent *entry) bool {
		if i >= t.count {
			return cb(*ent, t.buf)
		}

		for {
			if ckey == nil {
				eoff := binary.BigEndian.Uint32(t.cbuf[offset:])

				var ok bool
				cent, ok = readEntry(eoff, t.cbuf)
				if !ok {
					err = Error.New("invalid loaded entry buffer")
					return false
				}

				ckey = cent.readKey(t.cbuf)
			}

			switch bytes.Compare(ckey, ent.readKey(t.buf)) {
			case -1: // ckey is earlier. send it off
				if !cb(cent, t.cbuf) {
					return false
				}
				ckey, i, offset = nil, i+1, offset+4

			case 0: // same. use entry because it's new
				if !cb(*ent, t.buf) {
					return false
				}
				return true // move to next entry

			case 1: // ent is earlier. send it off
				if !cb(*ent, t.buf) {
					return false
				}
			}
		}
	})
	if err != nil {
		return err
	}

	// send one off if we already loaded it
	if ckey != nil {
		if !cb(cent, t.cbuf) {
			return nil
		}
		i, offset = i+1, offset+4
	}

	// finish off the cbuf
	for ; i < t.count; i, offset = i+1, offset+4 {
		eoff := binary.BigEndian.Uint32(t.cbuf[offset:])
		ent, ok := readEntry(eoff, t.cbuf)
		if !ok {
			return Error.New("invalid loaded entry buffer")
		} else if !cb(ent, t.cbuf) {
			return nil
		}
	}

	return nil
}

// Flush iterates over all of the entries in the node, but only returning the key and
// the pivot. The pivot returned by the callback is stored as the entries pivot. If
// a pivot of zero is returned, the entry is dropped. Returns any error from the callback.
func (t *T) Flush(cb func(key []byte, pivot uint32) (uint32, error)) error {
	var (
		bu   btreeBulk
		nbuf = make([]byte, nodeHeaderSize)
	)

	var uerr error
	err := t.iter(func(ent entry, buf []byte) bool {
		key := ent.readKey(buf)

		var npivot uint32
		if npivot, uerr = cb(key, ent.pivot); uerr != nil || npivot == 0 {
			return false
		}

		value := ent.readValue(buf)
		ent.pivot = npivot
		ent.offset = uint32(len(nbuf))

		hdr := ent.header()
		nbuf = append(nbuf, hdr[:]...)
		nbuf = append(nbuf, key...)
		nbuf = append(nbuf, value...)

		bu.append(ent)
		return true
	})
	if err != nil {
		return Error.Wrap(err)
	}
	if uerr != nil {
		return Error.Wrap(err)
	}

	// use the bulk loaded btree and buffer
	t.buf = nbuf
	t.entries = bu.done()
	t.dirty = true
	t.count = 0
	t.cbuf = nil

	return nil
}
