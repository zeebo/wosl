package node

import (
	"fmt"
	"time"

	"github.com/zeebo/wosl/internal/pcg"
)

const (
	numbersShift = 16
	numbersSize  = 1 << numbersShift
	numbersMask  = numbersSize - 1
)

var (
	numbers [][]byte
	kilobuf = make([]byte, 4<<10)
	gen     = pcg.New(uint64(time.Now().UnixNano()), 0)
)

func init() {
	numbers = make([][]byte, numbersSize)
	for i := range numbers {
		numbers[i] = []byte(fmt.Sprint(gen.Intn(numbersSize)))
	}
}

func newEntry(buf *[]byte, key, value string) (entry, []byte) {
	ent := entry{
		key:     uint32(len(key)),
		value:   uint32(len(value)),
		kindOff: uint32(len(*buf)) << 8,
	}

	hdr := ent.header()
	*buf = append(*buf, hdr[:]...)
	*buf = append(*buf, key...)
	*buf = append(*buf, value...)

	return ent, []byte(*buf)
}
