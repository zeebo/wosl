package mon

import (
	"math/bits"
	"sync/atomic"
)

const (
	// 42 buckets allows a value up to 2^42 which covers 1hr in nanoseconds
	// 6 entries per bucket keeps a relative error of 1 / 2^6 or ~1.5%.
	histEntries      = 6
	histEntriesCount = 1 << histEntries
	histBuckets      = 42
	histCounts       = histEntriesCount * (histBuckets - histEntries)
)

// locate returns the bucket and entry for a given index.
func locate(idx uint) (uint, uint) {
	return idx >> histEntries, idx & (histEntriesCount - 1)
}

// lowerValue returns the smallest value that can be stored at the index.
func lowerValue(idx uint) int64 {
	bucket, entry := locate(idx)
	return (1<<bucket-1)<<histEntries + int64(entry<<bucket)
}

// middleValue returns the value between the smallest and largest that can be
// stored at the index.
func middleValue(idx uint) int64 {
	bucket, entry := locate(idx)
	return (1<<bucket-1)<<histEntries + int64(entry<<bucket) + (1 << bucket / 2)
}

// upperValue returns the largest value that can be stored at the index.
func upperValue(idx uint) int64 {
	bucket, entry := locate(idx)
	return (1<<bucket-1)<<histEntries + int64(entry<<bucket) + (1 << bucket)
}

// Histogram keeps track of an exponentially increasing range of buckets
// so that there is a consistent relative error per bucket.
type Histogram struct {
	current int64
	total   int64
	bcounts [histBuckets]int64
	counts  [histCounts]int64
}

// start informs the Histogram that a task is starting.
func (h *Histogram) start() { atomic.AddInt64(&h.current, 1) }

// done informs the Histogram that a task has completed in the given
// amount of nanoseconds.
func (h *Histogram) done(v int64) {
	atomic.AddInt64(&h.current, -1)

	v += histEntriesCount
	bucket := uint64(bits.Len64(uint64(v))) - histEntries - 1
	entry := uint64(v>>bucket) - histEntriesCount
	idx := bucket<<histEntries + entry

	if idx < histCounts && bucket < histBuckets {
		atomic.AddInt64(&h.bcounts[bucket], 1)
		atomic.AddInt64(&h.counts[idx], 1)
		atomic.AddInt64(&h.total, 1)
	}
}

// Current returns the number of active calls.
func (h *Histogram) Current() int64 { return atomic.LoadInt64(&h.current) }

// Total returns the number of completed calls.
func (h *Histogram) Total() int64 { return atomic.LoadInt64(&h.total) }

// For quantile, we compute a target value at the start. After that, when
// walking the counts, we are sure we'll still hit the target since the
// counts and totals monotonically increase. This means that the returned
// result might be slightly smaller than the real result, but since
// the call is so fast, it's unlikely to drift very much.

// Quantile returns an estimation of the qth quantile in [0, 1].
func (h *Histogram) Quantile(q float64) int64 {
	target, bucket, acc := int64(q*float64(h.Total())+0.5), uint(0), int64(0)

	// skip through buckets
	for ; bucket < histBuckets; bucket++ {
		bcount := atomic.LoadInt64(&h.bcounts[bucket])
		if acc+bcount >= target {
			break
		}
		acc += bcount
	}

	// add after buckets
	for idx := bucket * histEntriesCount; idx < histCounts; idx++ {
		acc += atomic.LoadInt64(&h.counts[idx])
		if acc >= target {
			return lowerValue(idx)
		}
	}

	// should be unreachable
	return upperValue(histCounts)
}

// When computing the average or variance, we don't do any locking.
// When we have finished adding up into the accumulator, we know the
// actual statistic has to be somewhere between acc / stotal and
// acc / etotal, because the counts and totals monotonically increase.
// We return the average of those bounds. Since we're dominated by
// cache misses, this doesn't cost much extra.

// Average returns an estimation of the average.
func (h *Histogram) Average() float64 {
	stotal, acc := float64(h.Total()), int64(0)

	for bucket := uint(0); bucket < histBuckets; bucket++ {
		// skip empty buckets
		if atomic.LoadInt64(&h.bcounts[bucket]) == 0 {
			continue
		}

		// add up non-empty buckets
		low, high := bucket*histEntriesCount, (bucket+1)*histEntriesCount
		for idx := low; idx < histCounts && idx < high; idx++ {
			if count := atomic.LoadInt64(&h.counts[idx]); count > 0 {
				acc += count * middleValue(idx)
			}
		}
	}

	etotal, facc := float64(h.Total()), float64(acc)
	return (facc/stotal + facc/etotal) / 2
}

// Variance returns an estimation of the variance.
func (h *Histogram) Variance() float64 {
	stotal, avg, acc := float64(h.Total()), h.Average(), 0.0

	for bucket := uint(0); bucket < histBuckets; bucket++ {
		// skip empty buckets
		if atomic.LoadInt64(&h.bcounts[bucket]) == 0 {
			continue
		}

		// add up non-empty buckets
		low, high := bucket*histEntriesCount, (bucket+1)*histEntriesCount
		for idx := low; idx < histCounts && idx < high; idx++ {
			if count := atomic.LoadInt64(&h.counts[idx]); count > 0 {
				dev := float64(middleValue(idx)) - avg
				acc += dev * dev * float64(count)
			}
		}
	}

	etotal, facc := float64(h.Total()), float64(acc)
	return (facc/stotal + facc/etotal) / 2
}
