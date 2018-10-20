package node

import "encoding/binary"

const (
	// the different kinds of entries. kindSentinel is used for the special
	// element that ensures there is a root for the entire structure.
	kindInsert = iota
	kindTombstone
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
	key     uint32
	value   uint32
	kindOff uint32 // kind is lower 8 bits of kindOff
}

// readEntry returns an entry from the beginning of the buf given that
// the buf is offset bytes in.
func readEntry(offset uint32, buf []byte) (entry, bool) {
	if len(buf) < entryHeaderSize {
		return entry{}, false
	}
	return entry{
		pivot:   uint32(binary.BigEndian.Uint32(buf[0:4])),
		key:     uint32(binary.BigEndian.Uint32(buf[4:8])),
		value:   uint32(binary.BigEndian.Uint32(buf[8:12])),
		kindOff: uint32(buf[12]) | uint32(offset)<<8,
	}, true
}

// size returns how many bytes the entry consumes
func (e entry) size() int64 { return entryHeaderSize + int64(e.key) + int64(e.value) }

// header returns an array of bytes containing the entry header.
func (e *entry) header() (hdr [entryHeaderSize]byte) {
	binary.BigEndian.PutUint32(hdr[0:4], uint32(e.pivot))
	binary.BigEndian.PutUint32(hdr[4:8], uint32(e.key))
	binary.BigEndian.PutUint32(hdr[8:12], uint32(e.value))
	hdr[12] = byte(e.kindOff)
	return hdr
}

func (e *entry) offset() uint32 { return e.kindOff >> 8 }

// readKey returns a slice of the buffer that contains the key.
func (e *entry) readKey(buf []byte) []byte {
	start := entryHeaderSize + e.offset()
	return buf[start : start+e.key]
}

// readValue returns a slice of the buffer that contains the value.
func (e *entry) readValue(buf []byte) []byte {
	start := entryHeaderSize + e.key + e.offset()
	return buf[start : start+e.value]
}
