package streamsort

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
)

// TestStoreSortsRandomInput puts 10 000 uniformly-random 32-byte keys
// (mirrors the per-entity workload — keys are keccak(slot_key) for the
// storage MPT) plus 32-byte values, then iterates and asserts:
//
//   - every Put landed in the iterator (no drops)
//   - the iterator yields keys in strictly ascending bytewise order
//   - each yielded value matches its key's Put value (no key→value drift)
//
// Doubles as a regression guard against drift in the tuning knobs in
// New (e.g. someone disabling the WAL flush hook and breaking the
// read-after-write contract).
func TestStoreSortsRandomInput(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	const N = 10_000
	r := rand.New(rand.NewPCG(0xdeadbeef, 0xcafe1234))

	type kv struct{ key, value [32]byte }
	entries := make([]kv, N)
	for i := range entries {
		var seed [16]byte
		binary.BigEndian.PutUint64(seed[0:], r.Uint64())
		binary.BigEndian.PutUint64(seed[8:], r.Uint64())
		entries[i].key = sha256.Sum256(append([]byte("k:"), seed[:]...))
		entries[i].value = sha256.Sum256(append([]byte("v:"), seed[:]...))
		if err := s.Put(entries[i].key[:], entries[i].value[:]); err != nil {
			t.Fatalf("Put[%d]: %v", i, err)
		}
	}

	want := make(map[[32]byte][32]byte, N)
	for _, e := range entries {
		want[e.key] = e.value
	}

	if err := s.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	var (
		got     int
		prev    [32]byte
		hasPrev bool
	)
	iterErr := s.Iterate(func(k, v []byte) error {
		if len(k) != 32 || len(v) != 32 {
			return fmt.Errorf("unexpected len(k)=%d len(v)=%d", len(k), len(v))
		}
		if hasPrev && bytes.Compare(prev[:], k) >= 0 {
			return fmt.Errorf("not sorted: prev=%x current=%x", prev, k)
		}
		var kk [32]byte
		copy(kk[:], k)
		wantV, ok := want[kk]
		if !ok {
			return fmt.Errorf("yielded unknown key %x", k)
		}
		if !bytes.Equal(wantV[:], v) {
			return fmt.Errorf("value mismatch for key %x: got %x want %x", k, v, wantV)
		}
		delete(want, kk)
		copy(prev[:], k)
		hasPrev = true
		got++
		return nil
	})
	if iterErr != nil {
		t.Fatalf("Iterate: %v", iterErr)
	}
	if got != N {
		t.Errorf("yielded %d entries, want %d", got, N)
	}
	if len(want) != 0 {
		t.Errorf("Iterate dropped %d keys", len(want))
	}
}

// TestStoreOpensAtLowestFormatMajorVersion pins New to the lowest format
// major version Pebble supports. A regression back to FormatNewest (or any
// version above FormatMostCompatible) reintroduces the format-version-ratchet
// fsync storm this const guards against — see the comment on New.
func TestStoreOpensAtLowestFormatMajorVersion(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()
	if got := s.db.FormatMajorVersion(); got != pebble.FormatMostCompatible {
		t.Errorf("FormatMajorVersion = %d, want %d (FormatMostCompatible)", got, pebble.FormatMostCompatible)
	}
}

// TestStoreLowFormatMajorVersionRoundTrip is the write+flush+iterate round
// trip Task 1 requires: at FormatMostCompatible, Put/Finalize/Iterate must
// still work and preserve ascending key order, exactly as at any other
// format version.
func TestStoreLowFormatMajorVersionRoundTrip(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	keys := []string{"c", "a", "e", "b", "d"}
	for _, k := range keys {
		if err := s.Put([]byte(k), []byte("v-"+k)); err != nil {
			t.Fatalf("Put(%s): %v", k, err)
		}
	}
	if err := s.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	var got []string
	if err := s.Iterate(func(k, v []byte) error {
		if want := "v-" + string(k); string(v) != want {
			return fmt.Errorf("value for %q = %q, want %q", k, v, want)
		}
		got = append(got, string(k))
		return nil
	}); err != nil {
		t.Fatalf("Iterate: %v", err)
	}
	want := []string{"a", "b", "c", "d", "e"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("Iterate order = %v, want %v", got, want)
	}
}

// TestStorePutAfterClose verifies Put returns an error (rather than
// panicking) after Close.
func TestStorePutAfterClose(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Put([]byte("k"), []byte("v")); err == nil {
		t.Error("Put after Close: expected error, got nil")
	}
}

// TestStoreIterateAfterClose verifies Iterate returns an error after
// Close.
func TestStoreIterateAfterClose(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Iterate(func(_, _ []byte) error { return nil }); err == nil {
		t.Error("Iterate after Close: expected error, got nil")
	}
}

// TestStoreCloseIdempotent verifies double-Close returns nil.
func TestStoreCloseIdempotent(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("second Close: got %v, want nil", err)
	}
}

// TestStoreIterateYieldErrorPropagates verifies yield's error short-
// circuits iteration and is returned verbatim.
func TestStoreIterateYieldErrorPropagates(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	for i := 0; i < 100; i++ {
		k := []byte{byte(i)}
		v := []byte{byte(i)}
		if err := s.Put(k, v); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	if err := s.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	sentinel := errors.New("stop now")
	gotErr := s.Iterate(func(_, _ []byte) error { return sentinel })
	if gotErr == nil || gotErr.Error() != sentinel.Error() {
		t.Errorf("Iterate: got %v, want %v", gotErr, sentinel)
	}
}

// TestStoreIterateRepeatable: two consecutive Iterate calls on the same
// Store must yield byte-identical sequences. The reth pipeline relies
// on this for AddLeaf offload — the worker iterates once to compute the
// storage root, the writer iterates again to drive MDBX writes.
func TestStoreIterateRepeatable(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	pairs := [][2][]byte{
		{[]byte("a"), []byte("alpha")},
		{[]byte("b"), []byte("beta")},
		{[]byte("c"), []byte("gamma")},
	}
	for _, p := range pairs {
		if err := s.Put(p[0], p[1]); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := s.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	collect := func() [][2][]byte {
		var out [][2][]byte
		if err := s.Iterate(func(k, v []byte) error {
			out = append(out, [2][]byte{append([]byte{}, k...), append([]byte{}, v...)})
			return nil
		}); err != nil {
			t.Fatalf("Iterate: %v", err)
		}
		return out
	}
	first := collect()
	second := collect()
	if len(first) != len(pairs) || len(second) != len(pairs) {
		t.Fatalf("iterate counts: first=%d second=%d want=%d", len(first), len(second), len(pairs))
	}
	for i := range first {
		if !bytes.Equal(first[i][0], second[i][0]) || !bytes.Equal(first[i][1], second[i][1]) {
			t.Errorf("multi-iterate divergence at %d:\n first=(%q,%q)\n second=(%q,%q)",
				i, first[i][0], first[i][1], second[i][0], second[i][1])
		}
	}
}

// TestStoreGetRoundTrip exercises the random-access Get path that the
// disk-backed commitment ctx.Account / ctx.Storage callbacks use.
//
// Asserts:
//   - Get returns the matching value for every Put key, byte-equal
//   - Get on an absent key returns (nil, nil) — NOT an error
//   - Get works both BEFORE any Iterate and AFTER it (no regression in
//     the Pebble-reuse path now that the flush moved to Finalize)
//   - Get returned slices are owned by the caller — modifying them
//     doesn't corrupt Pebble's internal state
func TestStoreGetRoundTrip(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	const N = 100
	puts := make(map[string][]byte, N)
	for i := 0; i < N; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		v := []byte(fmt.Sprintf("value-%04d-padding-for-larger-payload", i))
		puts[string(k)] = v
		if err := s.Put(k, v); err != nil {
			t.Fatalf("Put[%d]: %v", i, err)
		}
	}
	if err := s.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// Get BEFORE any Iterate.
	for k, want := range puts {
		got, err := s.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get(%q) before Iterate: %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("Get(%q) = %q, want %q", k, got, want)
		}
	}

	// Get on an absent key returns (nil, nil), NOT an error.
	got, err := s.Get([]byte("absent-key"))
	if err != nil {
		t.Errorf("Get(absent): err = %v, want nil", err)
	}
	if got != nil {
		t.Errorf("Get(absent): got = %q, want nil", got)
	}

	// Mutating a returned slice must NOT corrupt subsequent reads.
	const k = "key-0042"
	got, _ = s.Get([]byte(k))
	for i := range got {
		got[i] = 0xff
	}
	again, _ := s.Get([]byte(k))
	if !bytes.Equal(again, puts[k]) {
		t.Errorf("Get(%q) after caller-side mutation: got %q, want %q", k, again, puts[k])
	}

	// Get after Iterate also works (no double-flush regression).
	if err := s.Iterate(func(_, _ []byte) error { return nil }); err != nil {
		t.Fatalf("Iterate: %v", err)
	}
	got, _ = s.Get([]byte("key-0001"))
	if !bytes.Equal(got, puts["key-0001"]) {
		t.Errorf("Get after Iterate: got %q, want %q", got, puts["key-0001"])
	}
}

// --- WRITING → FINALIZED → CLOSED state machine regression tests ---

// TestPutAfterFinalizeErrors: Put-after-Finalize must return an error
// (not a panic) so the state-machine violation surfaces explicitly.
func TestPutAfterFinalizeErrors(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	if err := s.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put (pre-Finalize): %v", err)
	}
	if err := s.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	err = s.Put([]byte("k2"), []byte("v2"))
	if err == nil {
		t.Fatal("Put after Finalize: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "Finalize") {
		t.Errorf("Put error should mention Finalize; got %v", err)
	}
}

// TestGetBeforeFinalizeAutoFinalizes: Get without an explicit Finalize
// auto-finalizes and returns the value (single-phase caller convenience).
func TestGetBeforeFinalizeAutoFinalizes(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	if err := s.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get before Finalize: unexpected error: %v", err)
	}
	if string(got) != "v" {
		t.Errorf("Get = %q, want %q", got, "v")
	}
}

// TestIterateBeforeFinalizeAutoFinalizes: Iterate without an explicit
// Finalize auto-finalizes and yields the entries.
func TestIterateBeforeFinalizeAutoFinalizes(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	if err := s.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	n := 0
	err = s.Iterate(func(k, v []byte) error {
		n++
		if string(k) != "k" || string(v) != "v" {
			t.Errorf("entry = (%q,%q), want (k,v)", k, v)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate before Finalize: unexpected error: %v", err)
	}
	if n != 1 {
		t.Errorf("Iterate yielded %d entries, want 1", n)
	}
}

// TestFinalizeIdempotent: Finalize twice → both calls succeed; the
// underlying batch is committed exactly once (a second commit on a
// committing batch would panic).
func TestFinalizeIdempotent(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	if err := s.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Finalize(); err != nil {
		t.Fatalf("Finalize (first): %v", err)
	}
	if err := s.Finalize(); err != nil {
		t.Fatalf("Finalize (second): %v", err)
	}
	got, err := s.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Errorf("Get after double-Finalize: got %q, want %q", got, "v")
	}
}

// TestCloseBeforeFinalize: the Put-only-never-read lifecycle (e.g. an
// aborted writer goroutine) still cleans up correctly. Close flushes
// the residual batch under putMu without going through Finalize.
func TestCloseBeforeFinalize(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestConcurrentGetAfterFinalize is the Bug 1 regression bar. It
// reproduces the load shape that crashed the SPEC_TARGET_GB=1 bench:
// N writes, Finalize, then 64 goroutines all calling Get concurrently.
// Without the explicit Finalize + per-reader gating, the pre-Finalize
// streamsort would race on the shared *pebble.Batch.Commit and panic
// with "pebble: batch already committing". With Finalize the read path
// goes straight to pebble.DB.Get, which is thread-safe.
//
// Must run cleanly under `go test -race`.
func TestConcurrentGetAfterFinalize(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	const N = 1_000
	want := make(map[string][]byte, N)
	for i := 0; i < N; i++ {
		k := []byte(fmt.Sprintf("k%05d", i))
		v := []byte(fmt.Sprintf("v%05d", i))
		want[string(k)] = v
		if err := s.Put(k, v); err != nil {
			t.Fatalf("Put[%d]: %v", i, err)
		}
	}
	if err := s.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	const workers = 64
	const opsPer = 200
	var wg sync.WaitGroup
	wg.Add(workers)
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		go func(seed int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(seed)+1, 0xabcdef))
			for op := 0; op < opsPer; op++ {
				idx := int(r.Uint64() % N)
				k := []byte(fmt.Sprintf("k%05d", idx))
				got, err := s.Get(k)
				if err != nil {
					errCh <- fmt.Errorf("worker=%d op=%d Get(%q): %v", seed, op, k, err)
					return
				}
				if !bytes.Equal(got, want[string(k)]) {
					errCh <- fmt.Errorf("worker=%d op=%d Get(%q) = %q, want %q",
						seed, op, k, got, want[string(k)])
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// TestConcurrentIterateAfterFinalize: 8 goroutines each calling
// Iterate concurrently. Each must see the full sorted set with byte-
// identical values. Pebble's NewIter is documented as safe for
// concurrent callers (iterator.go:177-178); this test guards the
// contract from accidental shared-state regressions in streamsort.
//
// Must run cleanly under `go test -race`.
func TestConcurrentIterateAfterFinalize(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	const N = 500
	want := make(map[string]string, N)
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("k%04d", i)
		v := fmt.Sprintf("v%04d", i)
		want[k] = v
		if err := s.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := s.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	const workers = 8
	var wg sync.WaitGroup
	wg.Add(workers)
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		go func(id int) {
			defer wg.Done()
			seen := make(map[string]string, N)
			err := s.Iterate(func(k, v []byte) error {
				seen[string(k)] = string(v)
				return nil
			})
			if err != nil {
				errCh <- fmt.Errorf("worker=%d Iterate: %v", id, err)
				return
			}
			if len(seen) != N {
				errCh <- fmt.Errorf("worker=%d saw %d entries, want %d", id, len(seen), N)
				return
			}
			for k, v := range want {
				if seen[k] != v {
					errCh <- fmt.Errorf("worker=%d Iterate[%q] = %q, want %q", id, k, seen[k], v)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// TestCloseDrainsReaders validates that Close blocks until in-flight
// readers complete, per the Pebble contract (db.go:1557) that DB.Close
// must not race with any other DB method. Without the readers
// WaitGroup, Close could race with an in-flight Get/Iterate and corrupt
// the Pebble DB shutdown.
//
// Mechanism: start an Iterate that pauses inside yield via a channel;
// kick off Close in a separate goroutine; assert Close has NOT
// returned until we release the iterate. Run under `-race`.
func TestCloseDrainsReaders(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	yieldEntered := make(chan struct{})
	releaseYield := make(chan struct{})
	iterDone := make(chan error, 1)
	go func() {
		iterDone <- s.Iterate(func(_, _ []byte) error {
			yieldEntered <- struct{}{}
			<-releaseYield
			return nil
		})
	}()

	<-yieldEntered

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- s.Close()
	}()

	// Give Close a chance to (incorrectly) return early — if drain is
	// broken, this races; if drain is correct, Close stays blocked.
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned (err=%v) while Iterate is still running — readers.Wait drain is broken", err)
	default:
		// expected: Close is blocked on readers.Wait
	}

	close(releaseYield)
	if err := <-iterDone; err != nil {
		t.Errorf("Iterate returned err=%v", err)
	}
	if err := <-closeDone; err != nil {
		t.Errorf("Close returned err=%v", err)
	}
}

// TestCursorRoundRobin verifies the pull Cursor: round-robining several
// cursors yields every key, each store still in sorted order, and interleaves
// them — the property the commitment Touch relies on so chunk 0 spans all
// 16 nibble sub-stores.
func TestCursorRoundRobin(t *testing.T) {
	mk := func(keys ...string) *Store {
		s, err := New(t.TempDir())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		for _, k := range keys {
			if err := s.Put([]byte(k), []byte("v-"+k)); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		if err := s.Finalize(); err != nil {
			t.Fatalf("Finalize: %v", err)
		}
		return s
	}
	a := mk("a1", "a2", "a3")
	defer a.Close()
	b := mk("b1", "b2")
	defer b.Close()

	ca, err := a.NewCursor()
	if err != nil {
		t.Fatalf("NewCursor a: %v", err)
	}
	defer ca.Close()
	cb, err := b.NewCursor()
	if err != nil {
		t.Fatalf("NewCursor b: %v", err)
	}
	defer cb.Close()

	var order []string
	for {
		advanced := false
		for _, c := range []*Cursor{ca, cb} {
			if c.Valid() {
				order = append(order, string(c.Key()))
				c.Next()
				advanced = true
			}
		}
		if !advanced {
			break
		}
	}
	if got, want := strings.Join(order, ","), "a1,b1,a2,b2,a3"; got != want {
		t.Fatalf("round-robin order = %q, want %q", got, want)
	}
	for _, c := range []*Cursor{ca, cb} {
		if err := c.Err(); err != nil {
			t.Fatalf("cursor err: %v", err)
		}
	}
}

// BenchmarkStoreNewClose measures the fixed bootstrap cost of an empty
// Store — os.MkdirTemp + pebble.Open + pebble.Close + os.RemoveAll, with no
// Put or Iterate at all. This is the cost StorageRoot pays once per
// PreAlloc entity regardless of how many slots it has, so it should stay
// near a millisecond; a regression here (e.g. the format-version ratchet
// this package's FormatMajorVersion comment guards against) turns into
// hours at 150,000 entities.
func BenchmarkStoreNewClose(b *testing.B) {
	dir := b.TempDir()
	for i := 0; i < b.N; i++ {
		s, err := New(dir)
		if err != nil {
			b.Fatalf("New: %v", err)
		}
		if err := s.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
	}
}

// BenchmarkStoreNewCloseParallel runs the same empty-store bootstrap
// concurrently. Run with `-cpu 1,2,4,8` to reproduce the degradation a
// competing writer on the same volume causes: each Store's fsyncs (WAL
// marker moves, directory syncs) serialise in the shared filesystem
// journal, so wall time per empty store grows with concurrency even though
// no work is being done.
func BenchmarkStoreNewCloseParallel(b *testing.B) {
	dir := b.TempDir()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s, err := New(dir)
			if err != nil {
				b.Fatalf("New: %v", err)
			}
			if err := s.Close(); err != nil {
				b.Fatalf("Close: %v", err)
			}
		}
	})
}

// TestGetter verifies the reusable SeekGE Getter: present keys (ascending
// fast path) return their value; absent keys return nil.
func TestGetter(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()
	keys := []string{"a", "c", "e", "g"}
	for _, k := range keys {
		if err := s.Put([]byte(k), []byte("v-"+k)); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := s.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	g, err := s.NewGetter()
	if err != nil {
		t.Fatalf("NewGetter: %v", err)
	}
	defer g.Close()
	for _, k := range keys { // ascending → reused-iterator fast path
		v, err := g.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get(%s): %v", k, err)
		}
		if string(v) != "v-"+k {
			t.Fatalf("Get(%s) = %q, want v-%s", k, v, k)
		}
	}
	for _, k := range []string{"b", "z"} { // absent
		v, err := g.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get(%s): %v", k, err)
		}
		if v != nil {
			t.Fatalf("Get(absent %s) = %q, want nil", k, v)
		}
	}
}
