package wosl

import (
	"testing"
)

func BenchmarkHeight(b *testing.B) {
	h := uint32(0)
	x := uint64(gen.Uint32())<<32 | uint64(gen.Uint32())
	z, l := gen.Uint32(), gen.Uint32()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h += height(x, z, l)
	}
}
