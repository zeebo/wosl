package node

import (
	"fmt"
	"testing"

	"github.com/zeebo/wosl/internal/assert"
)

func TestBtreeBulk(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		var bu btreeBulk
		var buf []byte

		for i := 0; i < 1000; i++ {
			ent, _ := newEntry(&buf, fmt.Sprint(i), "")
			bu.append(ent)
		}

		bt := bu.done()

		i := 0
		bt.Iter(func(ent entry) bool {
			key := string(ent.readKey(buf))
			assert.Equal(t, key, fmt.Sprint(i))
			i++
			return true
		})
	})
}
