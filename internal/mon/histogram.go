package mon

import (
	"sync/atomic"
)

const (
	bufferShift = 8 // 256 elements
	bufferElems = 1 << bufferShift
	bufferMask  = bufferElems - 1
)

// Histogram is a ring histogram of durations that have been observed.
type Histogram struct {
	total   int64
	current int64
	durs    [bufferElems]int64
}

// start should be called before done, to keep track of concurrent executions.
func (h *Histogram) start() { atomic.AddInt64(&h.current, 1) }

// done stores the duration in the ring buffer, incrementing the count.
func (h *Histogram) done(dur int64) {
	loc := &h.durs[(atomic.AddInt64(&h.total, 1)-1)&bufferMask]
	atomic.StoreInt64(loc, dur)
	atomic.AddInt64(&h.current, -1)
}

// Total returns the amount of times a duration has been added to the histogram.
func (h *Histogram) Total() int64 { return atomic.LoadInt64(&h.total) }

// Current returns the amount of currently recording executions exist
func (h *Histogram) Current() int64 { return atomic.LoadInt64(&h.current) }

// dursLen returns the number of valid entries in the durs buffer.
func (h *Histogram) dursLen() int {
	n := h.Total()
	if n > bufferElems {
		return bufferElems
	}
	return int(n & bufferMask)
}

// Durations returns a copy of observed durations.
func (h *Histogram) Durations() []int64 {
	out := make([]int64, h.dursLen())
	for i := range out {
		out[i] = atomic.LoadInt64(&h.durs[i&bufferMask])
	}
	return out
}

// Average returns the average time in nanoseconds.
func (h *Histogram) Average() float64 {
	total := int64(0)
	n := h.dursLen()
	for i := 0; i < n; i++ {
		total += atomic.LoadInt64(&h.durs[i])
	}
	return float64(total) / float64(n)
}
