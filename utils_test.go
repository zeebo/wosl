package wosl

import (
	"math/rand"
	"testing"
)

func BenchmarkHeight(b *testing.B) {
	h := int16(0)
	x := uint64(rand.Int63())
	z, l := uint32(rand.Int31()), uint32(rand.Int31())

	for i := 0; i < b.N; i++ {
		h += height(x, z, l)
	}
}

func BenchmarkAlign8(b *testing.B) {
	b.ReportAllocs()

	s := int64(0)

	for i := 0; i < b.N; i++ {
		s += align8(int64(i))
	}
}
