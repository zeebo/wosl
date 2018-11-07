package entry

import (
	"testing"

	"github.com/zeebo/wosl/internal/assert"
)

func TestEntry(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		ent := New(make([]byte, 1), make([]byte, 2), true, 4)
		assert.Equal(t, ent.Pivot(), 0)
		assert.Equal(t, ent.Key(), 1)
		assert.Equal(t, ent.Value(), 2)
		assert.Equal(t, ent.Tombstone(), true)
		assert.Equal(t, ent.Offset(), 4)
	})
}
