package node

import "encoding/binary"

const (
	// the different kinds of entries. kindSentinel is used for the special
	// element that ensures there is a root for the entire structure.
	kindInsert = iota
	kindTombstone
)

const (
	keyBits   = 20
	keyMask   = 1<<keyBits - 1
	valueBits = 44
	valueMask = 1<<valueBits - 1
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
	keyVal  uint64 // key is lowest 20 bits, value is highest 44.
	kindOff uint32 // kind is lower 8 bits of kindOff
}

// newEntry constructs an entry with the given parameters all bitpacked.
func newEntry(pivot, key uint32, val uint64, offset uint32, kind uint8) entry {
	return entry{
		pivot:   pivot,
		keyVal:  val<<keyBits | uint64(key),
		kindOff: offset<<8 | uint32(kind),
	}
}

// readEntry returns an entry from the beginning of the buf given that
// the buf is offset bytes in.
func readEntry(offset uint32, buf []byte) (entry, bool) {
	if int64(len(buf)) < entryHeaderSize+int64(offset) {
		return entry{}, false
	}
	buf = buf[offset:]
	return entry{
		pivot:   uint32(binary.BigEndian.Uint32(buf[0:4])),
		keyVal:  uint64(binary.BigEndian.Uint64(buf[4:12])),
		kindOff: uint32(buf[12]) | uint32(offset)<<8,
	}, true
}

// size returns how many bytes the entry consumes
func (e entry) size() int64 { return entryHeaderSize + int64(e.key()) }

// header returns an array of bytes containing the entry header.
func (e *entry) header() (hdr [entryHeaderSize]byte) {
	binary.BigEndian.PutUint32(hdr[0:4], uint32(e.pivot))
	binary.BigEndian.PutUint64(hdr[4:12], uint64(e.keyVal))
	hdr[12] = e.kind()
	return hdr
}

// accessors for the bit packed fields

func (e *entry) key() uint32    { return uint32(e.keyVal & keyMask) }
func (e *entry) value() uint64  { return e.keyVal >> keyBits }
func (e *entry) offset() uint32 { return e.kindOff >> 8 }
func (e *entry) kind() uint8    { return uint8(e.kindOff) }

func (e *entry) setOffset(offset uint32) { e.kindOff = offset<<8 | uint32(e.kind()) }

// readKey returns a slice of the buffer that contains the key.
func (e *entry) readKey(buf []byte) []byte {
	start := entryHeaderSize + e.offset()
	return buf[start : start+e.key()]
}
