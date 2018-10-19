package node

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/zeebo/wosl/internal/assert"
)

const (
	numbersShift = 20
	numbersSize  = 1 << numbersShift
	numbersMask  = numbersSize - 1
)

var (
	numbers [][]byte
	kilobuf = make([]byte, 4<<10)
)

func init() {
	numbers = make([][]byte, numbersSize)
	for i := range numbers {
		numbers[i] = []byte(fmt.Sprint(rand.Intn(numbersSize)))
	}
}

func TestNode(t *testing.T) {
	t.Run("Insert", func(t *testing.T) {
		n := New(4<<20, 0, 0)

		for i := 0; i < 100; i++ {
			buf := []byte(fmt.Sprint(rand.Intn(100)))
			assert.That(t, n.Insert(buf, buf))
		}

		last := ""
		for i := 0; i < len(n.entries); i++ {
			key := string(n.entries[i].readKey(n.buf))
			value := string(n.entries[i].readValue(n.buf))

			assert.That(t, key > last)
			assert.Equal(t, key, value)

			last = key
		}
	})

	t.Run("Write", func(t *testing.T) {
		n1 := New(4<<20, 0, 0)
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
		assert.Equal(t, len(n1.entries), len(n2.entries))

		for i, ent1 := range n1.entries {
			ent2 := n2.entries[i]

			assert.Equal(t, string(ent1.readKey(n1.buf)), string(ent2.readKey(n2.buf)))
			assert.Equal(t, string(ent1.readValue(n1.buf)), string(ent2.readValue(n2.buf)))
		}
	})
}

func BenchmarkNode(b *testing.B) {
	// Generate a full node and its written form for some
	// of the benchmarks
	n := New(4<<20, 0, 0)
	for {
		d := numbers[rand.Intn(numbersSize)&numbersMask]
		if !n.Insert(d, kilobuf) {
			break
		}
	}
	buf := make([]byte, n.Capacity())
	assert.NoError(b, n.Write(buf))

	b.Run("Insert", func(b *testing.B) {
		n := New(4<<20, 0, 0)
		resets := 0
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			d := numbers[i&numbersMask]
			if !n.Insert(d, kilobuf) {
				n.reset()
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
