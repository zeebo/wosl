package mon

import (
	"sync"
	"testing"

	"github.com/zeebo/wosl/internal/assert"
)

func TestHistogram(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		his := new(Histogram)
		assert.Equal(t, his.Total(), 0)
		assert.Equal(t, his.Current(), 0)
		assert.DeepEqual(t, his.Durations(), []int64{})

		his.start()
		assert.Equal(t, his.Total(), 0)
		assert.Equal(t, his.Current(), 1)
		assert.DeepEqual(t, his.Durations(), []int64{})

		his.done(1)
		assert.Equal(t, his.Total(), 1)
		assert.Equal(t, his.Current(), 0)
		assert.DeepEqual(t, his.Durations(), []int64{1})
	})

	t.Run("Race", func(t *testing.T) {
		wg := new(sync.WaitGroup)
		his := new(Histogram)

		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < 1e6; i++ {
				his.start()
				his.done(1)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < 1e6; i++ {
				his.Durations()
			}
		}()

		wg.Wait()
	})
}

func BenchmarkHistogram(b *testing.B) {
	b.Run("Start+Done", func(b *testing.B) {
		his := new(Histogram)

		for i := 0; i < b.N; i++ {
			his.start()
			his.done(1)
		}
	})
}
