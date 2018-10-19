package wosl

import (
	"fmt"
	"math/rand"

	"github.com/zeebo/errs"
)

const (
	numbersShift = 20
	numbersSize  = 1 << numbersShift
	numbersMask  = numbersSize - 1
)

var (
	numbers [][]byte
	kilobuf = make([]byte, 4<<10)
)

func init() {
	numbers = make([][]byte, numbersSize)
	for i := range numbers {
		numbers[i] = []byte(fmt.Sprint(rand.Intn(numbersSize)))
	}
}

type memDisk struct {
	size   int32
	max    uint32
	blocks map[uint32][]byte
}

func newMemDisk(size int32) *memDisk {
	return &memDisk{
		size:   size,
		blocks: make(map[uint32][]byte),
	}
}

func (m memDisk) BlockSize() int32          { return m.size }
func (n memDisk) MaxBlock() (uint32, error) { return n.max, nil }

func (m memDisk) Read(block uint32) ([]byte, error) {
	return m.blocks[block], nil
}

func (m *memDisk) Write(block uint32, data []byte) error {
	if int32(len(data)) != m.size {
		return errs.New("bad block size")
	}

	m.blocks[block] = append([]byte(nil), data...)
	if block > m.max {
		m.max = block
	}

	return nil
}
