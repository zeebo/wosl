package wosl

import (
	"fmt"
	"time"

	"github.com/zeebo/errs"
	"github.com/zeebo/wosl/internal/node"
	"github.com/zeebo/wosl/internal/pcg"
	"github.com/zeebo/wosl/lease"
)

const (
	numbersShift = 20
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

//
// memory cache that never evicts
//

type memCache struct {
	disk   *memDisk
	nodes  map[uint32]*node.T
	leases map[uint32]int
	buf    []byte
	cb     func(*node.T, uint32) error
}

func newMemCache(size uint32) *memCache {
	m := &memCache{
		disk:   newMemDisk(size),
		nodes:  make(map[uint32]*node.T),
		leases: make(map[uint32]int),
		buf:    make([]byte, size),
	}
	m.cb = m.writeBack
	return m
}

var _ Cache = (*memCache)(nil)

func (m *memCache) Disk() Disk { return m.disk }

func (m *memCache) Flush() error {
	for block, n := range m.nodes {
		if !n.Dirty() {
			continue
		}
		if err := m.writeBack(n, block); err != nil {
			return errs.Wrap(err)
		}
	}
	return nil
}

func (m *memCache) writeBack(n *node.T, block uint32) error {
	m.leases[block]--
	switch leases := m.leases[block]; {
	case leases < 0:
		panic("lease counter mismatch")
	case leases == 0:
		delete(m.leases, block)
	default:
		return nil
	}

	if err := n.Write(m.buf); err != nil {
		return errs.Wrap(err)
	} else if err := m.disk.Write(block, m.buf); err != nil {
		return errs.Wrap(err)
	}
	return nil
}

func (m *memCache) Add(n *node.T, block uint32) {
	if _, ok := m.nodes[block]; ok {
		panic("node already exists in cache")
	}
	m.nodes[block] = n
}

func (m *memCache) Get(block uint32) (lease.T, error) {
	n, ok := m.nodes[block]
	if !ok {
		if buf, err := m.disk.Read(block); err != nil {
			return lease.T{}, errs.Wrap(err)
		} else if buf == nil {
			return lease.T{}, errs.New("get on unknown block: %d", block)
		} else if n, err = node.Load(buf); err != nil {
			return lease.T{}, errs.Wrap(err)
		}
	}
	m.leases[block]++
	return lease.New(n, block, m.cb), nil
}

//
// memory disk
//

type memDisk struct {
	size   uint32
	max    uint32
	blocks map[uint32][]byte
}

var _ Disk = (*memDisk)(nil)

func newMemDisk(size uint32) *memDisk {
	return &memDisk{
		size:   size,
		blocks: make(map[uint32][]byte),
	}
}

func (m *memDisk) BlockSize() uint32         { return m.size }
func (n *memDisk) MaxBlock() (uint32, error) { return n.max, nil }

func (m *memDisk) Read(block uint32) ([]byte, error) {
	return m.blocks[block], nil
}

func (m *memDisk) Delete(block uint32) error {
	delete(m.blocks, block)
	return nil
}

func (m *memDisk) Write(block uint32, data []byte) error {
	if uint32(len(data)) != m.size {
		return errs.New("bad block size")
	}

	m.blocks[block] = append([]byte(nil), data...)
	if block > m.max {
		m.max = block
	}

	return nil
}
