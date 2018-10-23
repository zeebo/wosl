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

// we require that keys are <= 1KB and that values are <= 1MB.
// that means we have 10 bits for keys, and 20 bits for values.
// pack the kind into 2 bits, and we use a uint32 for all of them.
// we use another uint32 to describe the offset into some stream
// that the key + value are stored. we use 4 more bytes to store
// the prefix of the key so that we can do comparisons on those
// without having to read the key in some cases.

const (
	keyShift = 0
	keyBits  = 10
	keyMask  = 1<<keyBits - 1

	valueShift = keyShift + keyBits
	valueBits  = 20
	valueMask  = 1<<valueBits - 1

	kindShift = valueShift + valueBits
	kindBits  = 2
	kindMask  = 1<<kindBits - 1
)

// kvk is a bit packed key/value/kind into 32 bits.
type kvk uint32

// newKVK returns an kvk with the values packed. It will truncate
// any values that would not fit.
func newKVK(key uint16, value uint32, kind uint8) kvk {
	return kvk(
		uint32(key&keyMask)<<keyShift |
			uint32(value&valueMask)<<valueShift |
			uint32(kind&kindMask)<<kindShift)
}

func (e kvk) key() uint16   { return uint16(e>>keyShift) & keyMask }
func (e kvk) value() uint32 { return uint32(e>>valueShift) & valueMask }
func (e kvk) kind() uint8   { return uint8(e>>kindShift) & kindMask }

const entryHeaderSize = (0 +
	4 + // kvk
	4 + // pivot
	4 + // prefix
	0)

// entry is kept in sorted order in a node's memory buffer.
type entry struct {
	kvk            // 10 bits of key, 20 bits of value, 2 bits of kind.
	pivot  uint32  // 0 means no pivot: there is no block 0.
	prefix [4]byte // first four bytes of the key
	offset uint32  // offset into the stream
}

// newEntry constructs an entry with the given parameters all bitpacked.
func newEntry(key, value []byte, kind uint8, offset uint32) entry {
	var prefix [4]byte
	copy(prefix[:], key)

	return entry{
		pivot:  0,
		kvk:    newKVK(uint16(len(key)), uint32(len(value)), kind),
		prefix: prefix,
		offset: offset,
	}
}

// readEntry returns an entry from the beginning of the buf given that
// the buf is offset bytes in.
func readEntry(offset uint32, buf []byte) (entry, bool) {
	if int64(len(buf)) < entryHeaderSize+int64(offset) {
		return entry{}, false
	}
	buf = buf[offset:]

	var prefix [4]byte
	copy(prefix[:], buf[8:12])

	return entry{
		kvk:    kvk(binary.LittleEndian.Uint32(buf[0:4])),
		pivot:  uint32(binary.LittleEndian.Uint32(buf[4:8])),
		prefix: prefix,
		offset: offset,
	}, true
}

// size returns how many bytes the entry consumes
func (e entry) size() int64 { return entryHeaderSize + int64(e.key()) + int64(e.value()) }

// header returns an array of bytes containing the entry header.
func (e entry) header() (hdr [entryHeaderSize]byte) {
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(e.kvk))
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(e.pivot))
	copy(hdr[8:12], e.prefix[:])
	return hdr
}

// readKey returns a slice of the buffer that contains the key.
func (e entry) readKey(buf []byte) []byte {
	return buf[e.offset : e.offset+uint32(e.key())]
}

// readKey returns a slice of the buffer that contains the value.
func (e entry) readValue(buf []byte) []byte {
	start := e.offset + uint32(e.key())
	return buf[start : start+e.value()]
}

// readEntry returns a byte containing the combined key and value.
func (e entry) readEntry(buf []byte) []byte {
	return buf[e.offset : e.offset+uint32(e.key())+e.value()]
}
