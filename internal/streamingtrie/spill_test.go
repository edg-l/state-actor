package streamingtrie

import (
	"strconv"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// spill_test.go is the equivalence gate for Drain's two backings (see the
// spillThreshold comment in streamingtrie.go): the in-memory sort-and-dedup
// path and the streamsort.Store spill path MUST produce byte-identical
// output for the same input, including when duplicate raw keys collide on
// the same keyHash and Pebble's last-write-wins semantics must be matched
// exactly.

// capturedRow is one (keyHash, rawKey, value) triple observed by a Sink.
// Comparable, so a slice of these can be turned into a multiset via a map.
type capturedRow struct {
	keyHash, rawKey, value common.Hash
}

// setSpillThreshold overrides the package-level spillThreshold and returns a
// func that restores it. Scope the restore tightly (defer it immediately)
// rather than via t.Cleanup, so multiple calls within one test don't stack
// on top of each other's already-mutated value.
func setSpillThreshold(n int) func() {
	orig := spillThreshold
	spillThreshold = n
	return func() { spillThreshold = orig }
}

// drainAndCollect runs StorageRoot over pairs with spillThreshold forced to
// threshold, capturing every (keyHash, rawKey, value) triple the Sink sees.
func drainAndCollect(t *testing.T, threshold int, pairs []kv) (common.Hash, []capturedRow) {
	t.Helper()
	restore := setSpillThreshold(threshold)
	defer restore()

	var rows []capturedRow
	sink := func(keyHash, rawKey, value common.Hash) error {
		rows = append(rows, capturedRow{keyHash, rawKey, value})
		return nil
	}
	root, err := StorageRoot(t.TempDir(), iterFromPairs(pairs), newStackTrieBuilder(), sink)
	if err != nil {
		t.Fatalf("StorageRoot(threshold=%d): %v", threshold, err)
	}
	return root, rows
}

// rowMultiset turns rows into a count-by-row map so two emissions can be
// compared order-free.
func rowMultiset(rows []capturedRow) map[capturedRow]int {
	m := make(map[capturedRow]int, len(rows))
	for _, r := range rows {
		m[r]++
	}
	return m
}

// assertRowMultisetsEqual fails the test if a and b differ as multisets.
func assertRowMultisetsEqual(t *testing.T, a, b []capturedRow) {
	t.Helper()
	ma, mb := rowMultiset(a), rowMultiset(b)
	if len(ma) != len(mb) {
		t.Fatalf("row multiset size mismatch: got %d, want %d", len(ma), len(mb))
	}
	for row, count := range ma {
		if mb[row] != count {
			t.Errorf("row %+v: got count %d, want %d", row, count, mb[row])
		}
	}
}

// TestDrain_InMemoryAndSpillPathsEquivalent forces the same input through
// both backings — a huge threshold (never spills) and a threshold of 1
// (spills after the first record) — and asserts identical roots and
// identical emitted-row multisets.
func TestDrain_InMemoryAndSpillPathsEquivalent(t *testing.T) {
	pairs := fixturePairs100()

	memRoot, memRows := drainAndCollect(t, 1<<30, pairs)
	spillRoot, spillRows := drainAndCollect(t, 1, pairs)

	if memRoot != spillRoot {
		t.Errorf("root mismatch: mem=%s spill=%s", memRoot.Hex(), spillRoot.Hex())
	}
	assertRowMultisetsEqual(t, memRows, spillRows)
}

// TestDrain_DuplicateKeysLastWriteWins pins the determinism-critical case:
// several raw keys collide (same rawKey drained more than once, e.g. via
// templates.Concat composing metadata/balances/allowances with no dedup).
// Both backings must resolve every collision to the LAST record drained for
// that key — Pebble's Set semantics — not the first, and not a merge.
func TestDrain_DuplicateKeysLastWriteWins(t *testing.T) {
	keyA := uint64SlotKey(42)
	keyB := uint64SlotKey(43)
	keyC := uint64SlotKey(44)
	pairs := []kv{
		{key: keyA, value: common.HexToHash("0x1")},
		{key: keyB, value: common.HexToHash("0x2")},
		{key: keyA, value: common.HexToHash("0x3")}, // overwrites the 0x1 above
		{key: keyC, value: common.HexToHash("0x4")},
		{key: keyA, value: common.HexToHash("0x5")}, // last write for keyA — must win
	}

	memRoot, memRows := drainAndCollect(t, 1<<30, pairs)
	spillRoot, spillRows := drainAndCollect(t, 1, pairs)

	if memRoot != spillRoot {
		t.Fatalf("root mismatch: mem=%s spill=%s", memRoot.Hex(), spillRoot.Hex())
	}
	assertRowMultisetsEqual(t, memRows, spillRows)

	for _, rows := range [][]capturedRow{memRows, spillRows} {
		var gotA *capturedRow
		for i := range rows {
			if rows[i].rawKey == keyA {
				if gotA != nil {
					t.Fatalf("key A appeared twice in emitted rows: %+v", rows)
				}
				gotA = &rows[i]
			}
		}
		if gotA == nil {
			t.Fatalf("key A missing from emitted rows: %+v", rows)
		}
		if gotA.value != common.HexToHash("0x5") {
			t.Errorf("key A value = %s, want 0x5 (last write)", gotA.value.Hex())
		}
	}
}

// TestDrain_ThresholdBoundary drives StorageRoot with counts just below, at,
// and just above a shrunk spillThreshold, checking every case against the
// materialize-and-sort ground truth (stackTrieRootMaterialised). This is the
// case the background diagnosis calls out explicitly: correctness must not
// depend on which side of the threshold an entity happens to fall on.
func TestDrain_ThresholdBoundary(t *testing.T) {
	const threshold = 8
	restore := setSpillThreshold(threshold)
	defer restore()

	for _, n := range []int{threshold - 1, threshold, threshold + 1, threshold + 5} {
		n := n
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			pairs := make([]kv, n)
			for i := range pairs {
				pairs[i] = kv{
					key:   uint64SlotKey(uint64(i) + 1000),
					value: common.BytesToHash([]byte{byte(i + 1), 0x00, 0xab}),
				}
			}
			got, err := StorageRoot(t.TempDir(), iterFromPairs(pairs), newStackTrieBuilder(), nil)
			if err != nil {
				t.Fatalf("StorageRoot: %v", err)
			}
			want := stackTrieRootMaterialised(pairs)
			if got != want {
				t.Errorf("n=%d: root mismatch:\n got  %s\n want %s", n, got.Hex(), want.Hex())
			}
		})
	}
}
