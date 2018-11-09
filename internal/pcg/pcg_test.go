package pcg

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestPCG(t *testing.T) {
	rng := New(2345, 2378)
	out := make([]uint32, 10)
	for i := range out {
		out[i] = rng.Uint32()
	}

	// this is for a right rotate
	// assert.DeepEqual(t, out, []uint32{
	// 	0xccca066b,
	// 	0x40cee775,
	// 	0x0df46902,
	// 	0x981fbe29,
	// 	0xfc8bfb85,
	// 	0xcfd9eef2,
	// 	0xa046c325,
	// 	0x31abe14c,
	// 	0xe29defb4,
	// 	0x160568cc,
	// })

	// this is for a left rotate
	assert.DeepEqual(t, out, []uint32{
		0xa066bccc,
		0xee77540c,
		0x69020df4,
		0x981fbe29,
		0xb85fc8bf,
		0xb3f67bbc,
		0xb0c96811,
		0xbe14c31a,
		0x38a77bed,
		0x5a330581,
	})
}

var (
	blackholeUint32  uint32
	blackholeFloat64 float64
)

func BenchmarkPCG(b *testing.B) {
	b.Run("Uint32", func(b *testing.B) {
		rng := New(2345, 2378)
		for i := 0; i < b.N; i++ {
			blackholeUint32 += rng.Uint32()
		}
	})

	b.Run("Float64", func(b *testing.B) {
		rng := New(2345, 2378)
		for i := 0; i < b.N; i++ {
			blackholeFloat64 += rng.Float64()
		}
	})
}
