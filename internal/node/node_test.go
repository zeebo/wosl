package node

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/zeebo/wosl/internal/assert"
)

func TestNode(t *testing.T) {
	t.Run("Insert", func(t *testing.T) {
		n := New(4<<20, 0)

		for i := 0; i < 100; i++ {
			buf := []byte(fmt.Sprint(rand.Intn(100)))
			assert.That(t, n.Insert(buf, buf))
		}

		last := ""
		n.entries.Iter(func(ent entry) bool {
			key := string(ent.readKey(n.buf))
			value := string(ent.readValue(n.buf))

			assert.That(t, key > last)
			assert.Equal(t, key, value)

			last = key
			return true
		})
	})

	t.Run("Write", func(t *testing.T) {
		n1 := New(4<<20, 0)
		for {
			d := numbers[rand.Intn(numbersSize)&numbersMask]
			if !n1.Insert(d, kilobuf) {
				break
			}
		}

		buf := make([]byte, n1.Capacity())
		assert.NoError(t, n1.Write(buf))

		n2, err := Load(buf)
		assert.NoError(t, err)

		var entries1, entries2 []entry
		n1.entries.Iter(func(ent entry) bool {
			entries1 = append(entries1, ent)
			return true
		})
		n2.entries.Iter(func(ent entry) bool {
			entries2 = append(entries2, ent)
			return true
		})

		assert.Equal(t, len(entries1), len(entries2))
		for i, ent1 := range entries1 {
			ent2 := entries2[i]

			assert.Equal(t, string(ent1.readKey(n1.buf)), string(ent2.readKey(n2.buf)))
			assert.Equal(t, string(ent1.readValue(n1.buf)), string(ent2.readValue(n2.buf)))
		}
	})
}

func BenchmarkNode(b *testing.B) {
	b.Run("Insert", func(b *testing.B) {
		run := func(b *testing.B, v []byte) {
			b.Helper()

			n := New(4<<20, 0)
			resets := 0

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				d := numbers[i&numbersMask]
				if !n.Insert(d, v) {
					n.Reset()
					resets++
				}
			}

			b.Log("iterations:", b.N, "resets:", resets)
		}

		b.Run("Large", func(b *testing.B) { run(b, kilobuf) })
		b.Run("Small", func(b *testing.B) { run(b, kilobuf[:16]) })
	})

	b.Run("Write", func(b *testing.B) {
		run := func(b *testing.B, v []byte) {
			b.Helper()

			writes := 0
			n := New(4<<20, 0)
			for {
				d := numbers[rand.Intn(numbersSize)&numbersMask]
				if !n.Insert(d, v) {
					break
				}
				writes++
			}
			buf := make([]byte, n.Capacity())

			b.SetBytes(int64(len(buf)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				n.Write(buf)
			}

			if b.N == 1 {
				b.Log("entries:", writes)
			}
		}

		b.Run("Large", func(b *testing.B) { run(b, kilobuf) })
		b.Run("Small", func(b *testing.B) { run(b, kilobuf[:16]) })
	})

	b.Run("Load", func(b *testing.B) {
		run := func(b *testing.B, v []byte) {
			b.Helper()

			writes := 0
			n := New(4<<20, 0)
			for {
				d := numbers[rand.Intn(numbersSize)&numbersMask]
				if !n.Insert(d, v) {
					break
				}
				writes++
			}
			buf := make([]byte, n.Capacity())
			assert.NoError(b, n.Write(buf))

			b.SetBytes(int64(len(buf)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				Load(buf)
			}

			if b.N == 1 {
				b.Log("entries:", writes)
			}
		}

		b.Run("Large", func(b *testing.B) { run(b, kilobuf) })
		b.Run("Small", func(b *testing.B) { run(b, kilobuf[:16]) })
	})
}
