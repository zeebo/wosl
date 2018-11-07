package node

import (
	"fmt"
	"testing"

	"github.com/zeebo/wosl/internal/assert"
	"github.com/zeebo/wosl/internal/node/entry"
)

func TestBulk(t *testing.T) {
	t.Run("Append", func(t *testing.T) {
		var bu Bulk

		for i := 0; i < 1000; i++ {
			key := []byte(fmt.Sprintf("%04d", i))
			assert.That(t, bu.Append(key, nil, false, 0))
		}
		n := bu.Done(0, 0)

		last, base := "", n.buf[n.base:]
		n.entries.Iter(func(ent *entry.T) bool {
			key := string(ent.ReadKey(base))
			assert.That(t, key > last)
			last = key
			return true
		})
	})
}

func BenchmarkBulk(b *testing.B) {
	b.Run("Append", func(b *testing.B) {
		run := func(b *testing.B, v []byte) {
			keys := make([][]byte, b.N)
			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("%08d", i))
			}

			var bu Bulk
			b.SetBytes(8 + int64(len(v)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if !bu.Fits(keys[i], v, bufferSize) {
					bu.Reset()
				}
				bu.Append(keys[i], v, true, 0)
			}
		}

		b.Run("32KB", func(b *testing.B) { run(b, megabuf) })
		b.Run("1KB", func(b *testing.B) { run(b, megabuf[:1<<10]) })
		b.Run("16B", func(b *testing.B) { run(b, megabuf[:1<<4]) })
		b.Run("0B", func(b *testing.B) { run(b, nil) })
	})
}
