package wosl

import (
	"fmt"
	"testing"

	"github.com/zeebo/wosl/internal/assert"
)

func TestWosl(t *testing.T) {
	m := newMemDisk(4 << 20)
	sl, err := New(0.5, m)
	assert.NoError(t, err)

	for i := 0; i < 20; i++ {
		assert.NoError(t, sl.Insert([]byte(fmt.Sprint(i)), nil))
	}
}

func BenchmarkWosl(b *testing.B) {
	m := newMemDisk(4 << 20)
	sl, err := New(0.5, m)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		sl.Insert(numbers[i&numbersMask], kilobuf)
	}
}
