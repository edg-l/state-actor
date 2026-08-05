package progress

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNilEntityMeterIsNoOp(t *testing.T) {
	out := captureLog(t, func() {
		m := NewEntityMeter(nil) // nil Reporter → nil meter
		require.Nil(t, m)
		w := m.Worker() // nil meter → nil worker
		require.Nil(t, w)
		w.Entity(time.Millisecond, time.Millisecond) // must not panic
	})
	assert.Empty(t, out)
}

func TestEntityMeterBatchesToSharedTotal(t *testing.T) {
	r := New()
	r.interval = time.Hour // suppress emission; we only check the totals
	m := NewEntityMeter(r)
	w := m.Worker()

	w.Entity(10*time.Millisecond, 20*time.Millisecond)
	assert.Equal(t, int64(1), m.total.Load())
	assert.Equal(t, (10 * time.Millisecond).Nanoseconds(), m.drainNanos.Load())
	assert.Equal(t, (20 * time.Millisecond).Nanoseconds(), m.iterateNanos.Load())

	w.Entity(30*time.Millisecond, 40*time.Millisecond)
	assert.Equal(t, int64(2), m.total.Load())
	assert.Equal(t, (40 * time.Millisecond).Nanoseconds(), m.drainNanos.Load())
	assert.Equal(t, (60 * time.Millisecond).Nanoseconds(), m.iterateNanos.Load())
}

func TestEntityMeterEmitsDrainIterateSplit(t *testing.T) {
	r := New()
	r.interval = time.Millisecond
	out := captureLog(t, func() {
		r.Stage("phase 0")
		// Push the baseline into the past so the first Entity is eligible to emit.
		r.lastNano.Store(time.Now().Add(-time.Second).UnixNano())
		w := NewEntityMeter(r).Worker()
		w.Entity(5*time.Millisecond, 7*time.Millisecond)
	})
	assert.Contains(t, out, "entities")
	assert.Contains(t, out, "drain")
	assert.Contains(t, out, "iterate")
	assert.NotContains(t, out, "ETA") // count-only: no percentage / ETA
	assert.NotContains(t, out, "%")
}

// TestEntityMeterConcurrent is the -race guard: many workers calling Entity
// concurrently must funnel into the shared totals without data races, and
// the total must equal the exact number of Entity calls made.
func TestEntityMeterConcurrent(t *testing.T) {
	r := New()
	r.interval = time.Hour
	m := NewEntityMeter(r)

	const workers = 8
	const perWorker = 500

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := m.Worker()
			for range perWorker {
				w.Entity(time.Microsecond, time.Microsecond)
			}
		}()
	}
	wg.Wait()

	want := int64(workers * perWorker)
	assert.Equal(t, want, m.total.Load())
	assert.Equal(t, want*time.Microsecond.Nanoseconds(), m.drainNanos.Load())
	assert.Equal(t, want*time.Microsecond.Nanoseconds(), m.iterateNanos.Load())
}
