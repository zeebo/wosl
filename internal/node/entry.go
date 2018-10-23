package node

import (
	"encoding/binary"
)

const (
	// the different kinds of entries. kindSentinel is used for the special
	// element that ensures there is a root for the entire structure.
	kindInsert = iota
	kindTombstone
)

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

	kindShift = valueShift + valueBits
	kindBits  = 2
	kindMask  = 1<<kindBits - 1
)

const entryHeaderSize = (0 +
	4 + // kvk
	4 + // pivot
	4 + // prefix
	0)

// entry is kept in sorted order in a node's memory buffer.
type entry struct {
	kvk    uint32  // bitpacked key+value+kind
	pivot  uint32  // 0 means no pivot: there is no block 0.
	prefix [4]byte // first four bytes of the key
	offset uint32  // offset into the stream
}

// newEntry constructs an entry with the given parameters all bitpacked.
func newEntry(key, value []byte, kind uint8, offset uint32) entry {
	var prefix [4]byte
	copy(prefix[:], key)

	kvk := uint32(len(key)&keyMask)<<keyShift |
		uint32(len(value)&valueMask)<<valueShift |
		uint32(kind&kindMask)<<kindShift

	return entry{
		kvk:    kvk,
		pivot:  0,
		prefix: prefix,
		offset: offset,
	}
}

// key returns how many bytes of key there are.
func (e entry) key() uint32 { return uint32(e.kvk>>keyShift) & keyMask }

// value returns how many bytes of value there are.
func (e entry) value() uint32 { return uint32(e.kvk>>valueShift) & valueMask }

// kind returns the entry kind.
func (e entry) kind() uint8 { return uint8(e.kvk>>kindShift) & kindMask }

// size returns how many bytes the entry consumes as.
func (e entry) size() uint64 { return entryHeaderSize + uint64(e.key()) + uint64(e.value()) }

// header returns an array of bytes containing the entry header.
func (e entry) header() (hdr [entryHeaderSize]byte) {
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(e.kvk))
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(e.pivot))
	copy(hdr[8:12], e.prefix[:])
	return hdr
}

// readKey returns a slice of the buffer that contains the key.
func (e entry) readKey(buf []byte) []byte {
	return buf[e.offset : e.offset+e.key()]
}

// readKey returns a slice of the buffer that contains the value.
func (e entry) readValue(buf []byte) []byte {
	start := e.offset + e.key()
	return buf[start : start+e.value()]
}

// readEntry returns a byte containing the combined key and value.
func (e entry) readEntry(buf []byte) []byte {
	return buf[e.offset : e.offset+e.key()+e.value()]
}
