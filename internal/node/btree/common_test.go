package btree

import (
	"fmt"
	"time"

	"github.com/zeebo/wosl/internal/node/entry"
	"github.com/zeebo/wosl/internal/pcg"
)

const (
	numbersShift  = 16
	numbersSize   = 1 << numbersShift
	numbersMask   = numbersSize - 1
	numbersLength = 5
)

var (
	numbers [][]byte
	gen     = pcg.New(uint64(time.Now().UnixNano()), 0)
	megabuf = make([]byte, 1<<15-1) // 32KB - 1
)

func init() {
	numbers = make([][]byte, numbersSize)
	for i := range numbers {
		for len(numbers[i]) != numbersLength {
			numbers[i] = []byte(fmt.Sprint(gen.Intn(numbersSize)))
		}
	}
}

func appendEntry(buf *[]byte, key, value string) (entry.T, []byte) {
	ent := entry.New([]byte(key), []byte(value), false, uint32(len(*buf)))
	*buf = append(*buf, key...)
	*buf = append(*buf, value...)
	return ent, *buf
}
