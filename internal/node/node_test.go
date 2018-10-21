package node

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/zeebo/wosl/internal/assert"
)

const bufferSize = 1 << 20

func TestNode(t *testing.T) {
	t.Run("Insert", func(t *testing.T) {
		n := New(0, 0)

		for i := 0; i < 100; i++ {
			buf := []byte(fmt.Sprint(gen.Intn(100)))
			assert.That(t, n.Insert(buf, 0))
		}

		last := ""
		n.entries.Iter(func(ent *entry) bool {
			key := string(ent.readKey(n.buf))
			assert.That(t, key > last)
			last = key
			return true
		})
	})

	t.Run("Write+Load", func(t *testing.T) {
		run := func(t *testing.T, count uint64) {
			t.Helper()

			n1 := New(0, 0)
			for n := uint64(0); count == 0 || n < count; n++ {
				n1.Insert(numbers[gen.Intn(numbersSize)&numbersMask], 0)
				if n1.Length() > bufferSize {
					break
				}
			}

			n2, err := Load(n1.Write(nil))
			assert.NoError(t, err)

			var keys1 []string
			var values1 []uint64
			n1.iter(func(ent entry, buf []byte) bool {
				keys1 = append(keys1, string(ent.readKey(buf)))
				values1 = append(values1, ent.value())
				return true
			})

			var keys2 []string
			var values2 []uint64
			n2.iter(func(ent entry, buf []byte) bool {
				keys2 = append(keys2, string(ent.readKey(buf)))
				values2 = append(values2, ent.value())
				return true
			})

			assert.Equal(t, len(keys1), len(keys2))
			assert.Equal(t, len(values2), len(values2))

			for i := 0; i < len(keys1); i++ {
				assert.Equal(t, keys1[i], keys2[i])
				assert.Equal(t, values1[i], values2[i])
			}
		}

		t.Run("10", func(t *testing.T) { run(t, 10) })
		t.Run("Full", func(t *testing.T) { run(t, 0) })
	})
}

func BenchmarkNode(b *testing.B) {
	b.Run("Insert", func(b *testing.B) {
		n := New(0, 0)
		resets := 0

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			n.Insert(numbers[i&numbersMask], 0)
			if n.Length() > bufferSize {
				n.Reset()
				resets++
			}
		}

		b.Log("iterations:", b.N, "resets:", resets)
	})

	b.Run("Write", func(b *testing.B) {
		run := func(b *testing.B, n *T) {
			buf := n.Write(nil)

			b.SetBytes(int64(len(buf)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				n.Write(buf)
			}
		}

		writes := 0
		fresh := New(0, 0)
		for {
			fresh.Insert(numbers[rand.Intn(numbersSize)&numbersMask], 0)
			if fresh.Length() > bufferSize {
				break
			}
			writes++
		}
		loaded, err := Load(fresh.Write(nil))
		assert.NoError(b, err)

		if b.N == 1 {
			b.Log("entries:", writes)
		}

		b.Run("Fresh", func(b *testing.B) { run(b, fresh) })
		b.Run("Loaded", func(b *testing.B) { run(b, loaded) })
	})

	b.Run("Load", func(b *testing.B) {
		b.Skip("Too cheap to matter anymore")

		writes := 0
		n := New(0, 0)
		for {
			n.Insert(numbers[rand.Intn(numbersSize)&numbersMask], 0)
			if n.Length() > bufferSize {
				break
			}
			writes++
		}
		buf := n.Write(nil)

		b.SetBytes(int64(len(buf)))
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			Load(buf)
		}

		if b.N == 1 {
			b.Log("entries:", writes)
		}
	})
}
