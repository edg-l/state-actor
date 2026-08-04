# Regenerating genesis_dump.json

`genesis_dump.json` is a golden fixture produced by running the real ethrex
`add_initial_state` path against `genesis.json` and dumping every RocksDB
column family as hex-encoded JSON. It must be regenerated whenever the ethrex
storage schema changes.

## Pinned commit

```
lambdaclass/ethrex @ 55433c2
```

A pre-release commit, not a tagged release: it is what the ethpandaops
`glamsterdam-devnet-7` image was built from, and the latest release (v23.0.0)
predates two changes state-actor has to match. This is the same pin as the e2e
boot image (`client/ethrex/e2e_test.go`) and as the column-family list in
`internal/ethrex/constants.go`. All three move together.

`account_trie_nodes` and `storage_trie_nodes` are byte-identical from v13.0.0
(commit 318ec2888) through this commit, each step verified by regenerating and
diffing against the previous dump. What moved:

- `chain_data[0x80]` gained fork fields (`hegotaTime`, upstream in v21.0.0 via
  ethrex#6326).
- `bad_blocks` arrived in v22.0.0 (ethrex#6948, empty at genesis).
- `state_history` arrived after v23.0.0 (empty at genesis).
- `account_codes` values carry a JUMPDEST bitmap rather than an RLP list of u32
  offsets (ethrex#7095). ethrex still reads the older form — `decode_jumpdests`
  branches on the RLP item header and rebuilds the bitmap from the bytecode when
  it finds a list — so this is a size and representativeness change, not a
  compatibility break.

`STORE_SCHEMA_VERSION` reached 3 at v16 and has not moved at this commit.
ethrex#7095 bumps it to 4 on its own branch, gated by a no-op `migrate_3_to_4`;
that bump is not in this build, so `metadata.json` stays at 3.

Any ethrex build that bumps `STORE_SCHEMA_VERSION`, adds or removes a column
family, or changes the key layout of `account_trie_nodes`,
`storage_trie_nodes`, `account_codes`, or `account_code_metadata` requires
regenerating the dump and re-reviewing the Go codec in `internal/ethrex/`.
A CF added upstream must also be added to `Tables`; ethrex's
`drop_obsolete_cfs` deletes any CF it finds that its own `TABLES` does not
list, so `Tables` must never run ahead of this pin.

## Steps

1. Check out ethrex at the pinned commit:

   ```sh
   git clone https://github.com/lambdaclass/ethrex
   cd ethrex
   git checkout 55433c2
   ```

2. Copy the dump harness into ethrex's examples directory:

   ```sh
   cp /path/to/state-actor/internal/ethrex/testdata/gen/sa_dump.rs \
       crates/storage/examples/sa_dump.rs
   ```

3. Run the harness, pointing it at `genesis.json`:

   ```sh
   cargo run -p ethrex-storage --features rocksdb --example sa_dump -- \
       /path/to/state-actor/internal/ethrex/testdata/gen/genesis.json \
       /tmp/ethrex-sa-datadir \
       /tmp/genesis_dump.json
   ```

4. Copy the output back:

   ```sh
   cp /tmp/genesis_dump.json \
       /path/to/state-actor/internal/ethrex/testdata/genesis_dump.json
   ```

5. Run the golden tests to confirm all rows still match:

   ```sh
   go test ./internal/ethrex/...
   ```

   All tests must pass before committing the updated fixture.

## Notes

- `/tmp` is tmpfs on some systems; use a disk-backed path for the datadir if
  disk space is limited (the RocksDB datadir grows to ~10 MB for this genesis).
- The `sa_dump.rs` harness reopens the store read-only after a 200 ms sleep to
  let the background FKV generator thread exit cleanly. Do not remove that sleep.
- `genesis.json` sets chainId 1337 with one EOA, one storage contract (3 slots),
  and one JUMPDEST contract. Changing `genesis.json` also invalidates the dump.
- This approach mirrors the regeneration note in `internal/reth/doc.go`.
