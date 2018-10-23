package node

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/zeebo/wosl/internal/assert"
)

func TestBtree(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		var set = map[string]bool{}
		var buf []byte
		var bt btree

		for i := 0; i < 100000; i++ {
			d := string(numbers[gen.Intn(numbersSize)&numbersMask])
			set[d] = true
			bt.Insert(appendEntry(&buf, d, ""))
		}

		assert.Equal(t, bt.entries, len(set))

		last := ""
		bt.Iter(func(ent *entry) bool {
			key := string(ent.readKey(buf))
			assert.That(t, last < key)
			assert.That(t, set[key])
			delete(set, key)
			last = key
			return true
		})
	})

	t.Run("Random", func(t *testing.T) {
		var set = map[string]bool{}
		var entries []entry
		var buf []byte
		var bt btree

		for i := 0; i < 100000; i++ {
			d := string(numbers[gen.Intn(numbersSize)&numbersMask])
			set[d] = true
			ent, _ := appendEntry(&buf, d, "")
			entries = append(entries, ent)
		}
		rand.Shuffle(len(entries), func(i, j int) {
			entries[i], entries[j] = entries[j], entries[i]
		})
		for _, ent := range entries {
			bt.Insert(ent, buf)
		}

		assert.Equal(t, bt.entries, len(set))

		last := ""
		bt.Iter(func(ent *entry) bool {
			key := string(ent.readKey(buf))
			assert.That(t, last < key)
			assert.That(t, set[key])
			delete(set, key)
			last = key
			return true
		})
	})

	t.Run("Write+Load", func(t *testing.T) {
		var set = map[string]bool{}
		var buf []byte
		var bt btree

		for i := 0; i < 100000; i++ {
			d := string(numbers[gen.Intn(numbersSize)&numbersMask])
			set[d] = true
			bt.Insert(appendEntry(&buf, d, d))
		}

		data := bt.write(nil)
		bt, err := loadBtree(data)
		assert.NoError(t, err)

		assert.Equal(t, bt.entries, len(set))

		last := ""
		bt.Iter(func(ent *entry) bool {
			key := string(ent.readKey(buf))
			value := string(ent.readValue(buf))
			assert.Equal(t, key, value)
			assert.That(t, last < key)
			assert.That(t, set[key])
			delete(set, key)
			last = key
			return true
		})
	})

	t.Run("Bugs", func(t *testing.T) {
		if payloadEntries != 3 {
			t.Skip("Test requires payloadEntries to be 3")
		}

		t.Run("One", func(t *testing.T) {
			var buf []byte
			var bt btree

			bt.Insert(appendEntry(&buf, "A", ""))
			bt.Insert(appendEntry(&buf, "F", ""))
			bt.Insert(appendEntry(&buf, "D", ""))
			bt.Insert(appendEntry(&buf, "C", ""))
			bt.Insert(appendEntry(&buf, "E", ""))
			bt.Insert(appendEntry(&buf, "G", ""))
			bt.Insert(appendEntry(&buf, "B", ""))
			bt.Insert(appendEntry(&buf, "A", ""))

			assert.Equal(t, bt.entries, 7)
		})

		t.Run("Two", func(t *testing.T) {
			var buf []byte
			var bt btree

			bt.Insert(appendEntry(&buf, "A", ""))
			bt.Insert(appendEntry(&buf, "F", ""))
			bt.Insert(appendEntry(&buf, "D", ""))
			bt.Insert(appendEntry(&buf, "D", ""))
			bt.Insert(appendEntry(&buf, "C", ""))
			bt.Insert(appendEntry(&buf, "A", ""))
			bt.Insert(appendEntry(&buf, "C", ""))
			bt.Insert(appendEntry(&buf, "E", ""))
			bt.Insert(appendEntry(&buf, "B", ""))

			assert.Equal(t, bt.entries, 6)
		})
	})
}

func BenchmarkBtree(b *testing.B) {
	b.Run("Insert", func(b *testing.B) {
		b.Run("Sorted", func(b *testing.B) {
			var buf []byte

			ents := make([]entry, b.N)
			for i := range ents {
				ents[i], _ = appendEntry(&buf, fmt.Sprintf("%08d", i), "")
			}

			var bt btree
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				bt.Insert(ents[i], buf)
			}
		})

		b.Run("Random", func(b *testing.B) {
			var buf []byte

			ents := make([]entry, b.N)
			for i := range ents {
				key := string(numbers[gen.Intn(numbersSize)&numbersMask])
				ents[i], _ = appendEntry(&buf, key, "")
			}

			var bt btree
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				bt.Insert(ents[i], buf)
			}
		})
	})

	b.Run("Write", func(b *testing.B) {
		run := func(b *testing.B, n int) {
			var bt btree
			var buf []byte

			for i := 0; i < n; i++ {
				key := string(numbers[gen.Intn(numbersSize)&numbersMask])
				ent, _ := appendEntry(&buf, key, "")
				bt.Insert(ent, buf)
			}
			out := bt.write(nil)

			b.SetBytes(int64(len(out)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				out = bt.write(out)
			}
		}

		b.Run("100", func(b *testing.B) { run(b, 100) })
		b.Run("1K", func(b *testing.B) { run(b, 1000) })
		b.Run("10K", func(b *testing.B) { run(b, 10000) })
		b.Run("100K", func(b *testing.B) { run(b, 100000) })
	})

	b.Run("Load", func(b *testing.B) {
		run := func(b *testing.B, n int) {
			var bt btree
			var buf []byte

			for i := 0; i < n; i++ {
				key := string(numbers[gen.Intn(numbersSize)&numbersMask])
				ent, _ := appendEntry(&buf, key, "")
				bt.Insert(ent, buf)
			}
			out := bt.write(nil)

			b.SetBytes(int64(len(out)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				bt, _ = loadBtree(out)
			}
		}

		b.Run("100", func(b *testing.B) { run(b, 100) })
		b.Run("1K", func(b *testing.B) { run(b, 1000) })
		b.Run("10K", func(b *testing.B) { run(b, 10000) })
		b.Run("100K", func(b *testing.B) { run(b, 100000) })
	})
}
