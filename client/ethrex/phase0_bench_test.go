package ethrex_test

import (
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	ethrexinternal "github.com/ethereum/state-actor/internal/ethrex"
	"github.com/ethereum/state-actor/internal/streamingtrie"
	"github.com/ethereum/state-actor/internal/templates"
)

// BenchmarkPhase0StorageRoot reproduces the exact per-entity call
// runPhase0Storage makes (phase0_cgo.go): drain one entity's storage
// through streamingtrie into a StreamHashBuilder and take the root. The
// sink is a no-op — production routes rows into a per-worker RocksDB
// WriteBatch, an already-amortized cost — so this isolates the
// streamingtrie/streamsort shape the Phase-0 bottleneck lives in.
//
// Swept over slot counts 10/100/1k/10k/100k, the signal to watch is a FLAT
// region at the low end: per-entity cost should scale with slot count, and a
// flat region at 10-1k slots means fixed per-entity bootstrap cost (a
// streamsort.New Pebble instance, pre-spill-threshold) dominates over the
// actual work.
func BenchmarkPhase0StorageRoot(b *testing.B) {
	addr := common.HexToAddress("0x00000000000000000000000000000000000001")

	for _, n := range []int{10, 100, 1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("slots=%d", n), func(b *testing.B) {
			workDir := b.TempDir()
			noopSink := ethrexinternal.NodeSink(func(_, _ []byte) error { return nil })

			for i := 0; i < b.N; i++ {
				storage := templates.SynthesizeSlots(int64(i), addr, "bench", n)
				hb := ethrexinternal.NewStreamHashBuilder(noopSink)
				if _, err := streamingtrie.StorageRoot(workDir, storage, hb, nil); err != nil {
					b.Fatalf("StorageRoot: %v", err)
				}
			}
		})
	}
}
