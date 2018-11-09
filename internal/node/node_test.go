package node

import (
	"fmt"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/wosl/internal/node/entry"
)

const bufferSize = 1 << 20

func TestNode(t *testing.T) {
	t.Run("Insert", func(t *testing.T) {
		n := New(0)

		for i := 0; i < 100; i++ {
			buf := []byte(fmt.Sprint(gen.Intn(100)))
			assert.That(t, n.Insert(buf, nil, 0))
		}

		last, base := "", n.buf[n.base:]
		n.entries.Iter(func(ent *entry.T) bool {
			key := string(ent.ReadKey(base))
			assert.That(t, key > last)
			last = key
			return true
		})
	})

	t.Run("Write+Load", func(t *testing.T) {
		run := func(t *testing.T, count uint64) {
			n1 := New(0)
			for n := uint64(0); count == 0 || n < count; n++ {
				d := numbers[gen.Intn(numbersSize)&numbersMask]
				n1.Insert(d, d, 0)
				if n1.Length() > bufferSize {
					break
				}
			}

			buf, err := n1.Write(nil)
			assert.NoError(t, err)
			n2, err := Load(buf)
			assert.NoError(t, err)

			var keys1, values1 []string
			base1 := n1.buf[n1.base:]
			n1.entries.Iter(func(ent *entry.T) bool {
				keys1 = append(keys1, string(ent.ReadKey(base1)))
				values1 = append(values1, string(ent.ReadValue(base1)))
				return true
			})

			var keys2, values2 []string
			base2 := n2.buf[n2.base:]
			n2.entries.Iter(func(ent *entry.T) bool {
				keys2 = append(keys2, string(ent.ReadKey(base2)))
				values2 = append(values2, string(ent.ReadValue(base2)))
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
		run := func(b *testing.B, v []byte) {
			n := New(0)

			b.SetBytes(numbersLength + int64(len(v)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				n.Insert(numbers[i&numbersMask], v, 0)
				if n.Length() > bufferSize {
					n.Reset()
				}
			}
		}

		b.Run("32KB", func(b *testing.B) { run(b, megabuf) })
		b.Run("1KB", func(b *testing.B) { run(b, megabuf[:1<<10]) })
		b.Run("16B", func(b *testing.B) { run(b, megabuf[:1<<4]) })
		b.Run("0B", func(b *testing.B) { run(b, nil) })
	})

	b.Run("Write", func(b *testing.B) {
		run := func(b *testing.B, v []byte) {
			n := New(0)
			for {
				n.Insert(numbers[gen.Intn(numbersSize)&numbersMask], v, 0)
				if n.Length() > bufferSize {
					break
				}
			}
			buf, err := n.Write(nil)
			assert.NoError(b, err)

			b.SetBytes(int64(len(buf)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				n.Write(buf)
			}
		}

		b.Run("32KB", func(b *testing.B) { run(b, megabuf) })
		b.Run("1KB", func(b *testing.B) { run(b, megabuf[:1<<10]) })
		b.Run("16B", func(b *testing.B) { run(b, megabuf[:1<<4]) })
		b.Run("0B", func(b *testing.B) { run(b, nil) })
	})

	b.Run("Load", func(b *testing.B) {
		run := func(b *testing.B, v []byte) {
			n := New(0)
			for {
				n.Insert(numbers[gen.Intn(numbersSize)&numbersMask], v, 0)
				if n.Length() > bufferSize {
					break
				}
			}
			buf, err := n.Write(nil)
			assert.NoError(b, err)

			b.SetBytes(int64(len(buf)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				Load(buf)
			}
		}

		b.Run("32KB", func(b *testing.B) { run(b, megabuf) })
		b.Run("1KB", func(b *testing.B) { run(b, megabuf[:1<<10]) })
		b.Run("16B", func(b *testing.B) { run(b, megabuf[:1<<4]) })
		b.Run("0B", func(b *testing.B) { run(b, nil) })
	})
}
