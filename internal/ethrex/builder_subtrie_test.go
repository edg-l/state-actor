package ethrex

import (
	"math/rand"
	"testing"
)

// builder_subtrie_test.go pins the split build (16 NewSubtrieBuilder instances
// partitioned by first nibble, combined with CombineSubtrieRefs) to the single
// whole-trie Builder.
//
// Both the root hash AND the emitted row multiset must match. The rows are the
// load-bearing half: they ARE the account_trie_nodes / flat-KV contents, so a
// split build that got the root right while emitting different paths would hand
// ethrex a trie it cannot walk. Reuses collectRows / randSortedKV from
// builder_reference_test.go, which already compare rows order-free.

// buildWholeTrie runs every leaf through one Builder.
func buildWholeTrie(t *testing.T, keys, vals [][]byte) (string, map[string]int) {
	t.Helper()
	sink, rows := collectRows()
	b := NewBuilder(sink)
	for i := range keys {
		if err := b.AddLeaf(keys[i], vals[i]); err != nil {
			t.Fatalf("whole AddLeaf: %v", err)
		}
	}
	root, err := b.Root()
	if err != nil {
		t.Fatalf("whole Root: %v", err)
	}
	return root.Hex(), rows
}

// buildSplitTrie partitions leaves by first nibble across 16 subtrie builders
// and combines their refs. keys must be sorted ascending.
func buildSplitTrie(t *testing.T, keys, vals [][]byte) (string, map[string]int) {
	t.Helper()
	sink, rows := collectRows()

	var refs [16][]byte
	for n := 0; n < 16; n++ {
		sb := NewSubtrieBuilder(sink, 1)
		any := false
		for i := range keys {
			if int(keys[i][0]) != n {
				continue
			}
			any = true
			if err := sb.AddLeaf(keys[i], vals[i]); err != nil {
				t.Fatalf("subtrie[%x] AddLeaf: %v", n, err)
			}
		}
		if !any {
			continue
		}
		ref, err := sb.SubtrieRef()
		if err != nil {
			t.Fatalf("subtrie[%x] SubtrieRef: %v", n, err)
		}
		refs[n] = ref
	}

	root, err := CombineSubtrieRefs(sink, refs)
	if err != nil {
		t.Fatalf("CombineSubtrieRefs: %v", err)
	}
	return root.Hex(), rows
}

// distinctFirstNibbles counts how many first nibbles the key set uses. The split
// build is only defined for >= 2; below that the root is not a depth-0 branch.
func distinctFirstNibbles(keys [][]byte) int {
	var seen [16]bool
	n := 0
	for _, k := range keys {
		if !seen[k[0]] {
			seen[k[0]] = true
			n++
		}
	}
	return n
}

func TestSubtrieSplitMatchesWholeBuild(t *testing.T) {
	rng := rand.New(rand.NewSource(0x5B117))

	// Short nibble lengths pack the keyspace densely, which is what produces
	// extension nodes directly above a subtrie root — the case where the split
	// build's terminal parent depth actually matters. 64 is the production length.
	lengths := []int{2, 3, 4, 5, 8, 16, 32, 64}

	checked := 0
	for iter := 0; iter < 4000; iter++ {
		L := lengths[rng.Intn(len(lengths))]
		count := 2 + rng.Intn(62) // 2..63 leaves
		keys, vals := randSortedKV(rng, count, L)
		if len(keys) < 2 || distinctFirstNibbles(keys) < 2 {
			continue
		}
		checked++

		wantRoot, wantRows := buildWholeTrie(t, keys, vals)
		gotRoot, gotRows := buildSplitTrie(t, keys, vals)

		if gotRoot != wantRoot {
			t.Fatalf("iter %d (L=%d n=%d): root mismatch\n whole=%s\n split=%s",
				iter, L, len(keys), wantRoot, gotRoot)
		}
		if len(gotRows) != len(wantRows) {
			t.Fatalf("iter %d (L=%d n=%d): distinct row count: whole=%d split=%d",
				iter, L, len(keys), len(wantRows), len(gotRows))
		}
		for row, want := range wantRows {
			if got := gotRows[row]; got != want {
				t.Fatalf("iter %d (L=%d n=%d): row %q count whole=%d split=%d",
					iter, L, len(keys), row, want, got)
			}
		}
	}
	if checked < 1000 {
		t.Fatalf("only %d cases exercised the split path; the generator is not producing >=2 distinct first nibbles often enough", checked)
	}
}

// TestCombineSubtrieRefsRejectsUnderpopulated locks the guard: with fewer than
// two populated nibbles the root is not a depth-0 branch, and encoding one
// anyway would produce a silently wrong root.
func TestCombineSubtrieRefsRejectsUnderpopulated(t *testing.T) {
	var none [16][]byte
	if _, err := CombineSubtrieRefs(nil, none); err == nil {
		t.Fatal("expected an error for zero populated nibbles")
	}
	var one [16][]byte
	one[3] = []byte{0x80}
	if _, err := CombineSubtrieRefs(nil, one); err == nil {
		t.Fatal("expected an error for one populated nibble")
	}
}
