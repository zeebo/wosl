package wosl

import (
	"testing"

	"github.com/zeebo/wosl/internal/assert"
)

const blockSize = 1 << 20

func TestWosl(t *testing.T) {
	m := newMemCache(blockSize)
	sl, err := New(m)
	assert.NoError(t, err)

	for i := 0; i < 20; i++ {
		assert.NoError(t, sl.Insert(numbers[i&numbersMask], nil))
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
