package wosl

import (
	"github.com/zeebo/wosl/internal/pcg"
)

// align8 returns the largest multiple of 8 less than size.
func align8(size int64) int64 {
	return size &^ 7
}

// height returns the height for the node given the success
// probabilities in zero and later. A success probability of
// 0 never succeeds, and 1 << 32 - 1 almost always succeeds.
func height(hash uint64, zero, later uint32) (h int16) {
	// This function BARELY makes it in the inliner (go1.11)
	// It'd be nice to not have to break the PCG abstraction
	// and to avoid a naked return, etc. But here we are.

	rng := pcg.T{
		State: hash,
		Inc:   1,
	}

next:
	if rng.Uint32() >= zero {
		return
	}

	h++
	zero = later
	goto next
}
