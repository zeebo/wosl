package btree

import (
	"fmt"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/wosl/internal/node/entry"
)

func TestBulk(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		var bu Bulk
		var buf []byte

		for i := 0; i < 1000; i++ {
			var ent entry.T
			ent, buf = appendEntry(&buf, fmt.Sprintf("%04d", i), "")
			bu.Append(ent)
		}

		bt := bu.Done()

		i := 0
		bt.Iter(func(ent *entry.T) bool {
			key := string(ent.ReadKey(buf))
			assert.Equal(t, key, fmt.Sprintf("%04d", i))
			i++
			return true
		})
	})

	t.Run("One", func(t *testing.T) {
		var bu Bulk
		var buf []byte

		ent, _ := appendEntry(&buf, "0", "")
		bu.Append(ent)
		bt := bu.Done()

		bt.Iter(func(ent *entry.T) bool {
			assert.Equal(t, string(ent.ReadKey(buf)), "0")
			return true
		})
	})

	t.Run("Zero", func(t *testing.T) {
		var bu Bulk
		bt := bu.Done()

		bt.Iter(func(ent *entry.T) bool {
			t.Fatal("expected no entries")
			return true
		})
	})
}

func BenchmarkBulk(b *testing.B) {
	b.Run("Basic", func(b *testing.B) {
		run := func(b *testing.B, n int) {
			var buf []byte

			ents := make([]entry.T, n)
			for i := range ents {
				ents[i], _ = appendEntry(&buf, fmt.Sprintf("%08d", i), "")
			}

			b.SetBytes(int64(n) * 8)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				var bu Bulk
				for i := range ents {
					bu.Append(ents[i])
				}
			}
		}

		b.Run("100", func(b *testing.B) { run(b, 100) })
		b.Run("1000", func(b *testing.B) { run(b, 1000) })
		b.Run("10000", func(b *testing.B) { run(b, 10000) })
		b.Run("100000", func(b *testing.B) { run(b, 100000) })
	})
}
