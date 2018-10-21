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

func appendEntry(buf *[]byte, key string, value uint64) (entry, []byte) {
	ent := newEntry(0, uint32(len(key)), value, uint32(len(*buf)), kindInsert)
	hdr := ent.header()
	*buf = append(*buf, hdr[:]...)
	*buf = append(*buf, key...)
	return ent, []byte(*buf)
}
