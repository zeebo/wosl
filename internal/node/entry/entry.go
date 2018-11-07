package entry

// we require that keys are < 32KB and that values are < 32KB.
// that means we have 15 bits for keys, and 15 bits for values.
// pack the kind into 2 bits, and we use a uint32 for all of them.
// we use another uint32 to describe the offset into some stream
// that the key + value are stored. we use 4 more bytes to store
// the prefix of the key so that we can do comparisons on those
// without having to read the key in some cases. this makes an
// entry a compact 16 bytes, allowing 4 to fit in a cache line.

const (
	KeyShift = 0
	KeyBits  = 15
	KeyMask  = 1<<KeyBits - 1

	ValueShift = KeyShift + KeyBits
	ValueBits  = 15
	ValueMask  = 1<<ValueBits - 1

	TombstoneShift = ValueShift + ValueBits
	TombstoneBits  = 1
	TombstoneMask  = 1<<TombstoneBits - 1
)

// T represents an entry in some key value store.
type T struct {
	Prefix [4]byte // first four bytes of the key
	kvt    uint32  // bitpacked key+value+tombstone
	pivot  uint32  // 0 means no pivot: there is no block 0.
	offset uint32  // offset into the stream
}

// New constructs an entry with the given parameters all bitpacked.
func New(key, value []byte, tombstone bool, offset uint32) T {
	var prefix [4]byte
	copy(prefix[:], key)

	t := 0
	if tombstone {
		t = 1
	}

	kvt := uint32(len(key)&KeyMask)<<KeyShift |
		uint32(len(value)&ValueMask)<<ValueShift |
		uint32(t&TombstoneMask)<<TombstoneShift

	return T{
		Prefix: prefix,
		kvt:    kvt,
		pivot:  0,
		offset: offset,
	}
}

// Key returns how many bytes of key there are.
func (e T) Key() uint32 { return uint32(e.kvt>>KeyShift) & KeyMask }

// Value returns how many bytes of value there are.
func (e T) Value() uint32 { return uint32(e.kvt>>ValueShift) & ValueMask }

// Tombstone returns true if the entry is a tombstone.
func (e T) Tombstone() bool { return uint8(e.kvt>>TombstoneShift)&TombstoneMask > 0 }

// Offset returns the offset of the entry.
func (e T) Offset() uint32 { return e.offset }

// SetOffset updates the offset of the entry.
func (e *T) SetOffset(offset uint32) { e.offset = offset }

// Pivot returns the pivot of the entry
func (e T) Pivot() uint32 { return e.pivot }

// SetPivot updates the pivot of the entry.
func (e *T) SetPivot(pivot uint32) { e.pivot = pivot }

// ReadKey returns a slice of the buffer that contains the key.
func (e T) ReadKey(buf []byte) []byte {
	return buf[e.offset : e.offset+e.Key()]
}

// ReadValue returns a slice of the buffer that contains the value.
func (e T) ReadValue(buf []byte) []byte {
	start := e.offset + e.Key()
	return buf[start : start+e.Value()]
}

// ReadEntry returns a byte containing the combined key and value.
func (e T) ReadEntry(buf []byte) []byte {
	return buf[e.offset : e.offset+e.Key()+e.Value()]
}
