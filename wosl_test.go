package wosl

import (
	"testing"

	"github.com/zeebo/wosl/internal/assert"
)

func TestWosl(t *testing.T) {
	m := newMemCache(4 << 20)
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
				sl.Insert(numbers[i&numbersMask], v)
			}
		}

		b.Run("Memory", func(b *testing.B) {
			b.Run("Large", func(b *testing.B) { run(b, newMemCache(4<<20), kilobuf) })
			b.Run("Small", func(b *testing.B) { run(b, newMemCache(4<<20), kilobuf[:16]) })
		})
	})
}
