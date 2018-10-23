package node

import (
	"fmt"
	"time"

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
	megabuf = make([]byte, 1<<19) // 512KB
)

func init() {
	numbers = make([][]byte, numbersSize)
	for i := range numbers {
		for len(numbers[i]) != numbersLength {
			numbers[i] = []byte(fmt.Sprint(gen.Intn(numbersSize)))
		}
	}
}

func appendEntry(buf *[]byte, key, value string) (entry, []byte) {
	ent := newEntry([]byte(key), []byte(value), kindInsert, uint32(len(*buf)))
	*buf = append(*buf, key...)
	*buf = append(*buf, value...)
	return ent, []byte(*buf)
}
