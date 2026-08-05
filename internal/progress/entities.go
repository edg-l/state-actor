package progress

import (
	"fmt"
	"sync/atomic"
	"time"
)

// EntityMeter aggregates a per-entity count, plus the cumulative Drain and
// IterateRoot wall time behind each entity, into shared totals and emits a
// throttled heartbeat through a Reporter. It exists because SlotMeter only
// sees the slots inside one entity's iterate step: a phase dominated by many
// small entities, each paying a fixed per-entity setup cost regardless of
// slot count, is otherwise invisible to the progress log.
//
// Unlike SlotWorker, EntityWorker does not batch its update behind a stride.
// SlotMeter batches because slot counts run into the billions on a hot inner
// loop, where even an occasional atomic bump is measurable. Entity counts
// top out in the hundreds of thousands, and each entity costs a full
// Drain + sort/spill + IterateRoot — orders of magnitude more work than an
// atomic add — so updating the shared totals on every entity is immaterial;
// Reporter.Tick's own interval already throttles how often a line prints.
//
// Construct one per phase with EntityMeter, hand each worker goroutine its
// own *EntityWorker via Worker, and call Entity once per completed entity. A
// nil *EntityMeter (built from a nil Reporter) is a no-op.
type EntityMeter struct {
	r            *Reporter
	total        atomic.Int64
	drainNanos   atomic.Int64
	iterateNanos atomic.Int64
}

// NewEntityMeter returns an EntityMeter, or nil when r is nil (the no-op
// fast path for library/test callers that don't wire progress).
func NewEntityMeter(r *Reporter) *EntityMeter {
	if r == nil {
		return nil
	}
	return &EntityMeter{r: r}
}

// EntityMeter is the method form of NewEntityMeter so callers can write
// cfg.Progress.EntityMeter() without importing this package directly (and
// without naming the *EntityMeter type).
func (r *Reporter) EntityMeter() *EntityMeter {
	return NewEntityMeter(r)
}

// Worker returns a handle for recording completed entities. EntityWorker
// holds no per-goroutine state (see the type doc), but callers still get one
// per goroutine for symmetry with SlotMeter.Worker.
func (m *EntityMeter) Worker() *EntityWorker {
	if m == nil {
		return nil
	}
	return &EntityWorker{m: m}
}

// EntityWorker records completed entities. A nil *EntityWorker is a no-op.
type EntityWorker struct {
	m *EntityMeter
}

// Entity records one completed entity along with how long its Drain and
// IterateRoot halves took, and emits a heartbeat showing the running
// per-entity average of each half — the split that tells a reader whether
// time is going into draining slots or into per-entity setup.
func (w *EntityWorker) Entity(drain, iterate time.Duration) {
	if w == nil {
		return
	}
	n := w.m.total.Add(1)
	drainTotal := w.m.drainNanos.Add(drain.Nanoseconds())
	iterateTotal := w.m.iterateNanos.Add(iterate.Nanoseconds())
	avgDrain := time.Duration(drainTotal / n)
	avgIterate := time.Duration(iterateTotal / n)
	w.m.r.Tick(n, 0, fmt.Sprintf("entities · %s/entity drain · %s/entity iterate", avgDrain, avgIterate))
}
