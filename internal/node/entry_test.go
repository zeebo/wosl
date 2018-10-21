package node

import (
	"testing"

	"github.com/zeebo/wosl/internal/assert"
)

func TestEntry(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		ent := newEntry(1, 2, 3, 4, 5)
		assert.Equal(t, ent.pivot, 1)
		assert.Equal(t, ent.key(), 2)
		assert.Equal(t, ent.value(), 3)
		assert.Equal(t, ent.offset(), 4)
		assert.Equal(t, ent.kind(), 5)
	})

	t.Run("Write+Read", func(t *testing.T) {
		ent1 := newEntry(1, 2, 3, 4, 5)
		hdr := ent1.header()
		ent2, ok := readEntry(4, hdr[:])
		assert.That(t, ok)
		assert.Equal(t, ent1, ent2)
	})
}
