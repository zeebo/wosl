package node

import (
	"testing"

	"github.com/zeebo/wosl/internal/assert"
)

func TestEntry(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		ent := newEntry(make([]byte, 1), make([]byte, 2), 3, 4)
		assert.Equal(t, ent.pivot, 0)
		assert.Equal(t, ent.key(), 1)
		assert.Equal(t, ent.value(), 2)
		assert.Equal(t, ent.kind(), 3)
		assert.Equal(t, ent.offset, 4)
	})

	t.Run("Write+Read", func(t *testing.T) {
		ent1 := newEntry(make([]byte, 1), make([]byte, 2), 3, 4)
		hdr := ent1.header()
		ent2, ok := readEntry(4, append(make([]byte, 4), hdr[:]...))
		assert.That(t, ok)
		assert.Equal(t, ent1, ent2)
	})
}
