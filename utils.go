package wosl

import (
	"github.com/zeebo/wosl/internal/pcg"
)

// height returns the height for the node given the success
// probabilities in zero and later. A success probability of
// 0 never succeeds, and 1 << 32 - 1 almost always succeeds.
func height(hash uint64, zero, later uint32) (h uint32) {
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
