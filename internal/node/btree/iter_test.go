package btree

import (
	"testing"

	"github.com/zeebo/wosl/internal/assert"
)

func TestIterator(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		var set = map[string]bool{}
		var buf []byte
		var bt T

		for i := 0; i < 100000; i++ {
			d := string(numbers[gen.Intn(numbersSize)&numbersMask])
			set[d] = true
			bt.Insert(appendEntry(&buf, d, ""))
		}

		assert.Equal(t, bt.count, len(set))

		last, iter := "", bt.Iterator()
		for iter.Next() {
			ent := iter.Entry()
			key := string(ent.ReadKey(buf))
			assert.That(t, last < key)
			assert.That(t, set[key])
			delete(set, key)
			last = key
		}

		assert.Equal(t, len(set), 0)
	})

	t.Run("Empty", func(t *testing.T) {
		iter := new(T).Iterator()
		assert.That(t, !iter.Next())
	})
}
