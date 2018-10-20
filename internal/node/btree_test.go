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

			ent, bu := newEntry(&buf, key, "")
			n.insertEntry(ent.readKey(buf), ent, bu)

			keys = append(keys, key)
			seen[key] = true
		}

		sort.Strings(keys)
		for _, key := range keys {
			t.Log(key)
		}

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
			bt.Insert(newEntry(&buf, d, "v"))
		}

		assert.Equal(t, bt.len, len(set))

		last := ""
		bt.Iter(func(ent entry) bool {
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
			ent, _ := newEntry(&buf, d, "v")
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
		bt.Iter(func(ent entry) bool {
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

		bt.Insert(newEntry(&buf, "A", ""))
		bt.Insert(newEntry(&buf, "F", ""))
		bt.Insert(newEntry(&buf, "D", ""))
		bt.Insert(newEntry(&buf, "C", ""))
		bt.Insert(newEntry(&buf, "E", ""))
		bt.Insert(newEntry(&buf, "G", ""))
		bt.Insert(newEntry(&buf, "B", ""))
		bt.Insert(newEntry(&buf, "A", ""))

		assert.Equal(t, bt.len, 7)
	})
}
