package node

import (
	"fmt"
	"testing"

	"github.com/zeebo/wosl/internal/assert"
)

func TestIterator(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		n := New(0)

		for i := 0; i < 100; i++ {
			buf := []byte(fmt.Sprint(gen.Intn(100)))
			assert.That(t, n.Insert(buf, nil))
		}

		last, iter := "", n.Iterator()
		for iter.Next() {
			key := string(iter.Key())
			assert.That(t, key > last)
			last = key
		}
	})
}
