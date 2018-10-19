package pcg

import (
	"testing"

	"github.com/zeebo/wosl/internal/assert"
)

func TestPCG(t *testing.T) {
	pi := New(2345, 2378)
	out := make([]uint32, 10)
	for i := range out {
		out[i] = pi.Uint32()
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

var blackholeUint32 uint32

func BenchmarkPCG(b *testing.B) {
	pi := New(2345, 2378)

	for i := 0; i < b.N; i++ {
		blackholeUint32 += pi.Uint32()
	}
}
