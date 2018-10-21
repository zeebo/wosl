package node

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/zeebo/wosl/internal/assert"
)

func TestBtree(t *testing.T) {
	t.Run("Node", func(t *testing.T) {
		var keys []string
		var seen = map[string]bool{}
		var buf []byte
		var n btreeNode

		for i := 0; i < payloadEntries-1; i++ {
			var key string
			for key == "" || seen[key] {
				key = fmt.Sprint(gen.Uint32())
			}

			ent, bu := appendEntry(&buf, key, 0)
			n.insertEntry(ent.readKey(buf), ent, bu)

			keys = append(keys, key)
			seen[key] = true
		}

		sort.Strings(keys)

		for i := uint8(0); i < n.count; i++ {
			assert.Equal(t, string(n.payload[i].readKey(buf)), keys[i])
		}
	})

	t.Run("Basic", func(t *testing.T) {
		var set = map[string]bool{}
		var buf []byte
		var bt btree

		for i := 0; i < 10000; i++ {
			d := string(numbers[rand.Intn(numbersSize)&numbersSize])
			set[d] = true
			bt.Insert(appendEntry(&buf, d, 0))
		}

		assert.Equal(t, bt.len, len(set))

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

		for i := 0; i < 10000; i++ {
			d := string(numbers[rand.Intn(numbersSize)&numbersSize])
			set[d] = true
			ent, _ := appendEntry(&buf, d, 0)
			entries = append(entries, ent)
		}
		rand.Shuffle(len(entries), func(i, j int) {
			entries[i], entries[j] = entries[j], entries[i]
		})
		for _, ent := range entries {
			bt.Insert(ent, buf)
		}

		assert.Equal(t, bt.len, len(set))

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

	t.Run("Bug_One", func(t *testing.T) {
		if payloadEntries != 3 {
			t.Skip("Test requires payloadEntries to be 3")
		}

		var buf []byte
		var bt btree

		bt.Insert(appendEntry(&buf, "A", 0))
		bt.Insert(appendEntry(&buf, "F", 0))
		bt.Insert(appendEntry(&buf, "D", 0))
		bt.Insert(appendEntry(&buf, "C", 0))
		bt.Insert(appendEntry(&buf, "E", 0))
		bt.Insert(appendEntry(&buf, "G", 0))
		bt.Insert(appendEntry(&buf, "B", 0))
		bt.Insert(appendEntry(&buf, "A", 0))

		assert.Equal(t, bt.len, 7)
	})
}
