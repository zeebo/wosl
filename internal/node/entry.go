package node

// we require that keys are < 32KB and that values are < 32KB.
// that means we have 15 bits for keys, and 15 bits for values.
// pack the kind into 2 bits, and we use a uint32 for all of them.
// we use another uint32 to describe the offset into some stream
// that the key + value are stored. we use 4 more bytes to store
// the prefix of the key so that we can do comparisons on those
// without having to read the key in some cases. this makes an
// entry a compact 16 bytes, allowing 4 to fit in a cache line.

const (
	keyShift = 0
	keyBits  = 15
	keyMask  = 1<<keyBits - 1

	valueShift = keyShift + keyBits
	valueBits  = 15
	valueMask  = 1<<valueBits - 1

	tombstoneShift = valueShift + valueBits
	tombstoneBits  = 1
	tombstoneMask  = 1<<tombstoneBits - 1
)

// Entry is kept in sorted order in a node's memory buffer.
type Entry struct {
	kvt    uint32  // bitpacked key+value+tombstone
	pivot  uint32  // 0 means no pivot: there is no block 0.
	prefix [4]byte // first four bytes of the key
	offset uint32  // offset into the stream
}

// newEntry constructs an Entry with the given parameters all bitpacked.
func newEntry(key, value []byte, tombstone bool, offset uint32) Entry {
	var prefix [4]byte
	copy(prefix[:], key)

	t := 0
	if tombstone {
		t = 1
	}

	kvt := uint32(len(key)&keyMask)<<keyShift |
		uint32(len(value)&valueMask)<<valueShift |
		uint32(t&tombstoneMask)<<tombstoneShift

	return Entry{
		kvt:    kvt,
		pivot:  0,
		prefix: prefix,
		offset: offset,
	}
}

// key returns how many bytes of key there are.
func (e Entry) key() uint32 { return uint32(e.kvt>>keyShift) & keyMask }

// value returns how many bytes of value there are.
func (e Entry) value() uint32 { return uint32(e.kvt>>valueShift) & valueMask }

// Tombstone returns true if the Entry is a tombstone.
func (e Entry) Tombstone() bool { return uint8(e.kvt>>tombstoneShift)&tombstoneMask > 0 }

// Pivot returns the pivot of the Entry
func (e Entry) Pivot() uint32 { return e.pivot }

// SetPivot updates the pivot of the Entry.
func (e *Entry) SetPivot(pivot uint32) { e.pivot = pivot }

// readKey returns a slice of the buffer that contains the key.
func (e Entry) readKey(buf []byte) []byte {
	return buf[e.offset : e.offset+e.key()]
}

// readKey returns a slice of the buffer that contains the value.
func (e Entry) readValue(buf []byte) []byte {
	start := e.offset + e.key()
	return buf[start : start+e.value()]
}

// readEntry returns a byte containing the combined key and value.
func (e Entry) readEntry(buf []byte) []byte {
	return buf[e.offset : e.offset+e.key()+e.value()]
}
