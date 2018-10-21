package wosl

import (
	"math/rand"
	"testing"
)

func BenchmarkHeight(b *testing.B) {
	h := uint32(0)
	x := uint64(rand.Int63())
	z, l := uint32(rand.Int31()), uint32(rand.Int31())

	for i := 0; i < b.N; i++ {
		h += height(x, z, l)
	}
}
