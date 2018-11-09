// +build ignore

package wosl

import (
	"fmt"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/wosl/internal/node"
	"github.com/zeebo/wosl/lease"
)

const blockSize = 1 << 15

func TestWosl(t *testing.T) {
	m := newMemCache(blockSize)
	sl, err := New(m)
	assert.NoError(t, err)

	for i := 0; i < 2000; i++ {
		assert.NoError(t, sl.Insert(numbers[i&numbersMask], kilobuf))
	}

	seen := map[uint32]bool{rootBlock: true}
	stack := []uint32{rootBlock}
	for len(stack) > 0 {
		block := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		n, le := sl.root, lease.T{}
		if block != rootBlock {
			le, err = m.Get(block)
			assert.NoError(t, err)
			n = le.Node()
		}

		node.Dump(n)
		n.Update(func(ent *node.Entry, key []byte) bool {
			fmt.Println(string(key), ent.Pivot())
			pivot := ent.Pivot()
			if pivot != 0 && !seen[pivot] {
				stack = append(stack, pivot)
				seen[pivot] = true
			}
			return true
		})
		if pivot := n.Pivot(); pivot != 0 && !seen[pivot] {
			stack = append(stack, pivot)
			seen[pivot] = true
		}

		le.Close()
	}
}

func BenchmarkWosl(b *testing.B) {
	b.Run("Insert", func(b *testing.B) {
		run := func(b *testing.B, cache Cache, v []byte) {
			b.Helper()

			sl, err := New(cache)
			assert.NoError(b, err)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := sl.Insert(numbers[i&numbersMask], v); err != nil {
					assert.NoError(b, err) // assert is expensive
				}
			}
		}

		b.Run("Memory", func(b *testing.B) {
			b.Run("Large", func(b *testing.B) { run(b, newMemCache(blockSize), kilobuf) })
			b.Run("Small", func(b *testing.B) { run(b, newMemCache(blockSize), kilobuf[:16]) })
		})
	})
}
