package node

import (
	"fmt"
	"sort"
	"testing"

	"github.com/zeebo/wosl/internal/assert"
)

func TestBtreeNode(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		var keys []string
		var seen = map[string]bool{}
		var buf []byte
		var n btreeNode

		for i := 0; i < payloadEntries-1; i++ {
			var key string
			for key == "" || seen[key] {
				key = fmt.Sprint(gen.Uint32())
			}

			ent, bu := appendEntry(&buf, key, "")
			n.insertEntry(ent.readKey(buf), ent, bu)

			keys = append(keys, key)
			seen[key] = true
		}

		sort.Strings(keys)

		for i := uint8(0); i < n.count; i++ {
			assert.Equal(t, string(n.payload[i].readKey(buf)), keys[i])
		}
	})
}
