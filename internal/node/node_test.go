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
	// Generate a full node and its written form for some
	// of the benchmarks
	n := New(4<<20, 0)
	for {
		d := numbers[rand.Intn(numbersSize)&numbersMask]
		if !n.Insert(d, kilobuf) {
			break
		}
	}
	buf := make([]byte, n.Capacity())
	assert.NoError(b, n.Write(buf))

	b.Run("Insert", func(b *testing.B) {
		n := New(4<<20, 0)
		resets := 0
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			d := numbers[i&numbersMask]
			if !n.Insert(d, kilobuf[:16]) {
				n.Reset()
				resets++
			}
		}

		b.Log(b.N, resets)
	})

	b.Run("Write", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			n.Write(buf)
		}
	})

	b.Run("Load", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			Load(buf)
		}
	})
}
