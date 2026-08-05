// Package streamingtrie computes a storage-trie root from a streaming
// per-entity slot iter, in bounded RAM regardless of slot count.
//
// Pipeline:
//
//  1. Drain (slotKey, value) into a streamsort.Store keyed by
//     keccak(slotKey). Zero-valued slots are skipped (canonical).
//  2. Iterate sorted; for each entry call the Sink (per-client DB row
//     writes), then feed (keyHash, trim+RLP(value)) to the HashBuilder.
//  3. Return HashBuilder.Root().
//
// RAM is bounded by streamsort.MemTableSize. Disk usage during the
// drain is O(slot_count × 96 B) in the temp Pebble dir.
//
// Producers that are pure functions of their inputs may be passed to
// StorageRoot multiple times and produce identical roots.
package streamingtrie

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"
	"slices"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"

	"github.com/ethereum/state-actor/internal/streamsort"
)

// spillThreshold is the number of records Drain buffers in memory before
// giving up on an in-memory sort and spilling to a streamsort.Store. Below
// it, Drain never calls streamsort.New — the fixed bootstrap cost of an
// empty Pebble instance (os.MkdirTemp + pebble.Open, ~24 fsyncs) is what
// makes small-storage entities (most PreAlloc accounts) dominated by setup
// rather than work.
//
// A record is a 32-byte key hash plus a 64-byte (rawKey||value) payload — 96
// bytes (see drainRecord). 1<<18 (262,144) records is ~24 MiB per worker,
// the same order of magnitude as the existing per-worker WriteBatch flush
// threshold (workerFlushThresholdBytes, 16 MiB, node_sink_cgo.go); at
// maxPhase0Workers=8 concurrent entities that adds ~192 MiB, far inside a
// ~50 GB ceiling against a 26.5 GB observed peak.
//
// A var, not a const, so tests can shrink it to exercise the spill boundary
// without buffering hundreds of thousands of records.
var spillThreshold = 1 << 18

// HashBuilder is the per-client streaming MPT builder contract.
// AddLeaf is called in keccak-ascending order with the 32-byte keyHash
// (already keccak'd by StorageRoot) and the RLP-encoded value bytes.
// Root is called once after iteration completes.
type HashBuilder interface {
	AddLeaf(keyHash common.Hash, valueRLP []byte) error
	Root() (common.Hash, error)
}

// Sink is invoked once per sorted (keyHash, rawKey, value) triple
// before the trie-leaf is appended. Non-nil error short-circuits.
type Sink func(keyHash, rawKey, value common.Hash) error

// StorageRoot drains storage into a streamsort.Store, walks the sorted
// output to drive Sink and HashBuilder in lockstep, and returns the
// storage trie root.
//
// workDir is passed through to streamsort.New (empty → os.TempDir()).
// storage MUST yield deterministically. hb MUST be freshly constructed.
// sink MAY be nil (root-only path).
//
// Equivalent to: Drain → IterateRoot → Close. Use the split form when
// the drain phase should run in a different goroutine from the
// iterate-with-sink phase (e.g. to parallelise drain across entities
// while serialising the write phase on a single DB-writer lock).
func StorageRoot(
	workDir string,
	storage iter.Seq2[common.Hash, common.Hash],
	hb HashBuilder,
	sink Sink,
) (common.Hash, error) {
	if hb == nil {
		return common.Hash{}, fmt.Errorf("streamingtrie: nil HashBuilder")
	}
	d, err := Drain(workDir, storage)
	if err != nil {
		return common.Hash{}, err
	}
	defer d.Close()
	return d.IterateRoot(hb, sink)
}

// drainRecord is one drained (keyHash, rawKey, value) triple, held as plain
// fields when Drain stays under spillThreshold instead of the packed
// 32+64-byte layout streamsort.Store uses on disk.
type drainRecord struct {
	keyHash common.Hash
	rawKey  common.Hash
	value   common.Hash
}

// Drained is a finished drain-phase result, backed by EITHER a sorted
// on-disk streamsort.Store OR an in-memory, already-sorted-and-deduped
// slice — see Drain. Either way it is ready to be replayed in keccak-
// ascending order via IterateRoot. The caller MUST Close it once the
// iterate phase is done.
//
// A Drained is owned by a single goroutine at a time; the drain
// goroutine may hand it off to a writer goroutine over a channel.
type Drained struct {
	store *streamsort.Store // non-nil once Drain has spilled to disk
	mem   []drainRecord     // used while store == nil; sorted + deduped
}

// Drain hashes every (rawKey, value) pair from storage, skips zero values,
// and buffers the rest as (keccak(rawKey), rawKey, value) records.
//
// While the buffer stays below spillThreshold, records stay in memory and
// no streamsort.Store is ever created — the common case, since most
// PreAlloc entities have far fewer than spillThreshold slots. If the
// source iterator ends first, the buffer is sorted stably by keyHash and,
// within each run of equal keys, only the LAST record is kept — matching
// Pebble's last-write-wins semantics for a duplicate Set (duplicate raw
// keys are reachable: templates.Concat composes ERC-20 metadata, balances,
// and allowances with no dedup).
//
// If the buffer reaches spillThreshold before the iterator ends, Drain
// creates a streamsort.Store, replays the buffered records into it via Put
// in the same order they were buffered, and continues draining directly
// into the store exactly as before. Because Put order is preserved
// end-to-end, Pebble's own last-write-wins resolves duplicates identically
// to the in-memory path — no separate dedup logic is needed once spilled.
//
// The returned Drained owns whichever backing it used and MUST be closed
// by the caller.
//
// nil storage is allowed and returns an empty (in-memory) Drained
// (IterateRoot will yield the empty-trie root).
func Drain(
	workDir string,
	storage iter.Seq2[common.Hash, common.Hash],
) (*Drained, error) {
	if storage == nil {
		return &Drained{}, nil
	}

	// buf grows via append's own doubling rather than pre-allocating
	// spillThreshold capacity up front — most entities have far fewer
	// slots than the threshold, and reserving the full ~24 MiB for every
	// one of them would defeat the point of avoiding a Pebble bootstrap.
	var buf []drainRecord
	var store *streamsort.Store
	var putErr error

	spillNow := func() error {
		s, err := streamsort.New(workDir)
		if err != nil {
			return fmt.Errorf("streamingtrie: streamsort.New: %w", err)
		}
		for _, r := range buf {
			var combined [64]byte
			copy(combined[0:32], r.rawKey[:])
			copy(combined[32:64], r.value[:])
			if err := s.Put(r.keyHash[:], combined[:]); err != nil {
				s.Close()
				return fmt.Errorf("streamingtrie: drain Put: %w", err)
			}
		}
		store = s
		buf = nil
		return nil
	}

	storage(func(k, v common.Hash) bool {
		if v == (common.Hash{}) {
			return true
		}
		keyHash := crypto.Keccak256Hash(k[:])

		if store != nil {
			var combined [64]byte
			copy(combined[0:32], k[:])
			copy(combined[32:64], v[:])
			if err := store.Put(keyHash[:], combined[:]); err != nil {
				putErr = fmt.Errorf("streamingtrie: drain Put: %w", err)
				return false
			}
			return true
		}

		buf = append(buf, drainRecord{keyHash: keyHash, rawKey: k, value: v})
		if len(buf) >= spillThreshold {
			if err := spillNow(); err != nil {
				putErr = err
				return false
			}
		}
		return true
	})
	if putErr != nil {
		if store != nil {
			store.Close()
		}
		return nil, putErr
	}
	if store != nil {
		return &Drained{store: store}, nil
	}

	// Never spilled: sort stably on the 32-byte key hash, then collapse each
	// run of equal keys down to its last record (see the last-write-wins
	// note above). Stable sort preserves each run's original relative
	// order, so the last element of a run is the record that was drained
	// last for that key.
	slices.SortStableFunc(buf, func(a, b drainRecord) int {
		return bytes.Compare(a.keyHash[:], b.keyHash[:])
	})
	deduped := buf[:0]
	for i, r := range buf {
		if i+1 < len(buf) && buf[i+1].keyHash == r.keyHash {
			continue // a later record with this key exists later in the run
		}
		deduped = append(deduped, r)
	}
	return &Drained{mem: deduped}, nil
}

// IterateRoot walks the drained records in keccak-ascending order,
// invoking sink (if non-nil) and HashBuilder.AddLeaf in lockstep, then
// returns the storage trie root. Callable exactly once per Drained;
// closing is the caller's responsibility.
func (d *Drained) IterateRoot(hb HashBuilder, sink Sink) (common.Hash, error) {
	if hb == nil {
		return common.Hash{}, fmt.Errorf("streamingtrie: nil HashBuilder")
	}

	process := func(keyHash, rawKey, value common.Hash) error {
		if sink != nil {
			if err := sink(keyHash, rawKey, value); err != nil {
				return fmt.Errorf("streamingtrie: sink: %w", err)
			}
		}

		valBytes := value[:]
		for len(valBytes) > 0 && valBytes[0] == 0 {
			valBytes = valBytes[1:]
		}
		valRLP, err := rlp.EncodeToBytes(valBytes)
		if err != nil {
			return fmt.Errorf("streamingtrie: rlp encode value: %w", err)
		}
		if err := hb.AddLeaf(keyHash, valRLP); err != nil {
			return fmt.Errorf("streamingtrie: AddLeaf: %w", err)
		}
		return nil
	}

	if d.store != nil {
		if err := d.store.Iterate(func(keyHashB, combinedB []byte) error {
			var keyHash, rawKey, value common.Hash
			copy(keyHash[:], keyHashB)
			copy(rawKey[:], combinedB[0:32])
			copy(value[:], combinedB[32:64])
			return process(keyHash, rawKey, value)
		}); err != nil {
			return common.Hash{}, err
		}
	} else {
		for _, r := range d.mem {
			if err := process(r.keyHash, r.rawKey, r.value); err != nil {
				return common.Hash{}, err
			}
		}
	}

	root, err := hb.Root()
	if err != nil {
		return common.Hash{}, fmt.Errorf("streamingtrie: Root: %w", err)
	}
	return root, nil
}

// Close releases the underlying backing, whichever one Drain used.
// Idempotent.
func (d *Drained) Close() {
	if d == nil {
		return
	}
	if d.store != nil {
		_ = d.store.Close()
		d.store = nil
	}
	d.mem = nil
}

func uint64SlotKey(slot uint64) common.Hash {
	var h common.Hash
	binary.BigEndian.PutUint64(h[24:32], slot)
	return h
}
