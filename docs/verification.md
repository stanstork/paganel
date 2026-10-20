# Cryptographic Migration Verification

Keyed Merkle receipts for post-migration integrity verification

---

## Table of Contents

- [What Verification Proves](#what-verification-proves)
- [What Verification Does Not Prove](#what-verification-does-not-prove)
  - [A receipt covers a table, not a run](#a-receipt-covers-a-table-not-a-run)
  - [Trust boundary](#trust-boundary)
- [Architecture](#architecture)
- [The Receipt](#the-receipt)
- [Reading a Result](#reading-a-result)
- [Canonical Row Serialization](#canonical-row-serialization)
- [Row Keys](#row-keys)
- [Merkle Tree Construction](#merkle-tree-construction)
- [State Persistence](#state-persistence)
  - [Storage footprint](#storage-footprint)
- [Verification Process](#verification-process)
- [CLI Usage](#cli-usage)
- [Cascade, Lanes, and Retries](#cascade-lanes-and-retries)
- [Edge Cases](#edge-cases)
- [Performance](#performance)
- [Future Extensions](#future-extensions)

---

## What Verification Proves

**The destination contains exactly the data that was written during migration.**

Conventional migration verification relies on row count comparison. Matching counts do not guarantee matching data: silent corruption, partial writes, network-level bit flips, and OOM kills mid-batch can all produce a destination with the correct row count but incorrect data.

Paganel hashes each post-transform row, keys that hash by the row's primary key, and commits the whole keyed set to a single Merkle root stored in a `VerificationReceipt`. The `verify` command re-reads the destination, rebuilds the same keyed set, and compares, reporting each difference by primary key.

This detects:

- Rows deleted from the destination after migration (**missing**)
- Rows inserted into the destination after migration (**extra**)
- Rows modified in the destination after migration (**changed**)
- Rows silently dropped or corrupted during the write phase

Because rows are keyed rather than positioned, the comparison is **order-independent**: nothing about how the migration ran (batch size, lane count, FK traversal order, retries) has to be replayed for verification to work.

The receipt's `table_root` is the portable half of the artifact: 32 bytes that commit to the entire table. It survives after the per-row hashes are deleted, and can be logged, recorded, or handed to an auditor. Retained somewhere outside the state directory, it also makes later tampering with the row-hash store detectable rather than silent (see [Trust boundary](#trust-boundary)).

## What Verification Does Not Prove

**Transform correctness is checked by unit and integration tests, not by verification.**

The hash is computed over post-transform output. Whether `lower(trim(email))` does the right thing, whether a `when` expression maps tiers correctly: that is validated by tests. Verification checks that the destination matches what was written, not whether what was written is semantically correct.

Verification also does not prove that the correct rows were selected from the source. If a `where` filter was wrong and selected the wrong rows, the destination hash will still match the stored receipt, because the receipt was computed from whatever was written.

**Self-attestation.** Both halves of the comparison are produced by Paganel: the receipt records what the producer computed, and verify re-reads what the destination holds. This is strong evidence against data loss, corruption, partial writes, and after-the-fact modification. It is not an end-to-end proof that source and destination agree; that needs a second commitment taken on the source side, which is out of scope here (see [Future Extensions](#future-extensions)).

### A receipt covers a table, not a run

A receipt commits the keyed set of rows one `apply --integrity` wrote, and
`verify` reads the destination table start to finish. Every destination key that
is not in the receipt is therefore reported as `extra`, and by default that is a
mismatch, whether the row is a tampered insert or was simply already there.

Three ordinary situations land on that path, none of them corruption:

- the destination table already held rows before the migration,
- the same pipeline ran twice to append more rows (each run resets its row-hash
  log, so the newer receipt covers only the newer rows), or
- two pipelines write into one destination table (receipts are per
  `pipeline:table`, so each run's rows are uncovered by the other's receipt).

`mode = "replace"` (TRUNCATE + INSERT) always verifies clean, because the
destination then holds exactly what the run wrote.

For the append cases, `verify --allow-extra` narrows the check to the rows the
receipt covers: uncovered rows are counted and reported in the result line, but
are not folded into the recomputed root, so the receipt's own rows still have to
reproduce `table_root` exactly and any `missing` or `changed` row among them
still fails. What is given up is the ability to detect rows inserted into the
destination after the migration; use the default when that matters.

### Trust boundary

The construction is cryptographic (collision-resistant hashing and a binding Merkle commitment), but what that buys you depends on where the root ends up.

Against **accident** the guarantee is unconditional. Silent truncation, partial writes, bit flips, a batch lost to an OOM kill: none of these can produce a row set that still folds to the recorded root, so verification catches them with no further precautions.

Against a **deliberate** local modification it is conditional. The receipt is unsigned and lives in the same state directory as the row hashes it commits to. Anyone able to rewrite the destination can also rewrite the row-hash store *and* the receipt, then recompute a root consistent with the result. Nothing in the file layout prevents that.

What closes the gap is retaining the root somewhere the modification cannot reach: a build log, a ticket, a commit, an append-only store. `apply` prints the full `table_root` when each receipt is written, `verify` prints it next to every result, and `pag receipt` (`--json` for tooling) prints the stored receipts again on demand; comparing a later run's root against a copy you kept is what turns "the data is self-consistent" into "the data is unchanged since the migration".

Two related limits, for completeness:

- **No authorship.** The root commits to content, not to who produced it. Anyone can compute a valid root for any data, so a receipt cannot distinguish one Paganel run from a forged file. Signing the root would be the fix; nothing here does that today.
- **No secret.** There is no keyed MAC, so the commitment is verifiable by anyone, which is what makes it auditable, and also why it proves nothing about provenance.

### Non-deterministic destination columns

The receipt records the values written from the source at apply time; `verify`
re-reads the destination and compares. So if a verified column's destination
value is generated non-deterministically rather than copied, the read-back will
not match the receipt and `verify` reports a **false mismatch**, even though the
data movement was correct. This happens when a destination column:

- has a non-deterministic default (`now()` / `CURRENT_TIMESTAMP`, `random()`,
  `uuid_generate_v4()`, …) that fires because the column isn't supplied from the
  source, or
- is regenerated on write by an `ON UPDATE CURRENT_TIMESTAMP` clause or a
  trigger (e.g. on an `on_conflict = "do_update"` upsert).

Paganel does not strip such defaults from the destination: a destination
column may legitimately need one, and a pre-existing destination table is outside
the migration's control. If a table has non-deterministic columns, expect
`verify` to flag it: the mismatch is in those generated columns, not in the
copied data.

> **Planned:** a future verification update will let you exclude such columns
> from row hashing, so tables with non-deterministic defaults can still be
> verified on their stable columns.

---

## Architecture

### Integration Point in the Pipeline

Hashing runs inside `BatchCoordinator` (`IntegrityState`), after `TransformService` applies all field mappings, computed columns, and type coercions, and before the batch is sent to the consumer channel.

```
Source (SnapshotReader)
  -> TransformService (field mapping, computed columns, coercions)
  -> integrity: hash + key each row, stream the pairs to the store
  -> BatchCoordinator (send batch to consumer)
  ↓ MPSC channel
Sink (BatchWriter)
  -> StateManager (checkpoint)
```

The engine holds no row hashes of its own: each batch's `(row key, row hash)` pairs are appended to the table's row-hash log and the batch is dropped. Appends are unordered: lanes write into the same log, in whatever order rows arrive. When every lane has finished, the log is sealed: sorted by key and deduplicated by external merge sort, then folded into one Merkle root per table, and the receipts are written.

The row-hash log's own footprint is flat as well; see [Row hashes](#row-hashes) for how, and [Performance](#performance) for the measurements.

### Crate Responsibilities

| Crate | Responsibility |
| --- | --- |
| `model` | Canonical encoding, keyed row hashing, streaming Merkle. No I/O. |
| `engine-processing` | Hash and key rows per batch, stream them to the store; fold roots and write receipts once the pipeline finishes. |
| `engine-state` | Persist receipts and keyed row hashes; provide sorted iteration over them. |
| `engine-verify` | Re-read the destination, stage its keyed hashes, merge-join against the receipt's set. |
| `cli` | Map `--integrity` to the run mode, invoke verification, print or write the report. |

### Data Flow: Migration (Write Path)

1. The orchestrator clears the pipeline's row hashes from the previous run.
2. Producer reads a page of rows from the source.
3. `TransformService` applies transforms (field mapping, computed columns, coercions).
4. The integrity stage groups rows by destination table and, per row, computes the canonical row hash and the canonical row key.
5. The batch's `(key, hash)` pairs are written to the row-hash store; the batch continues to the consumer.
6. Consumer writes to the destination.
7. After every lane finishes, the orchestrator streams each table's stored pairs in key order, folds them into a Merkle root, and writes one receipt per table.

### Data Flow: Verification (Read-Back Path)

1. Load the `VerificationReceipt` for each pipeline/table pair.
2. Re-read every destination row (keyset over the primary key, any deterministic full scan will do).
3. Hash and key each row with the receipt's own `column_order` and `key_columns`, staging the pairs under the verify scope.
4. Merge-join the two key-sorted streams: fold the destination side into `actual_root` and record every `missing` / `extra` / `changed` key in one pass.
5. Drop the staged verify-side hashes.
6. Return `Vec<VerificationResult>`, one result per table.

### Diagram

```
                          apply --integrity
  ╔═══════════════════════════════════════════════════════════════════╗
  ║  SOURCE          TRANSFORM          HASH + KEY          WRITE     ║
  ║                                                                   ║
  ║  ┌──────┐   ┌─────────────────┐                    ┌────────────┐ ║
  ║  │MySQL │─▶ │ TransformService│                    │ BatchWriter│ ║
  ║  └──────┘   └────────┬────────┘                    └────────────┘ ║
  ║                      │  rows[]                          ▲         ║
  ║             ┌────────▼─────────┐    Batch{}             │         ║
  ║             │  IntegrityState  │───────────────────────▶│         ║
  ║             │  hash + key rows │                                  ║
  ║             └─────────┬────────┘                                  ║
  ║   (any batch,         │  (key,hash) (key,hash) ...                ║
  ║    any lane,          ▼                                           ║
  ║    any order)   ┌──────────────────────────┐                      ║
  ║                 │  rowhash log (appended)  │  sorted at seal      ║
  ║                 └────────────┬─────────────┘                      ║
  ║                              │ streamed in key order              ║
  ║        all lanes done        ▼                                    ║
  ║                 ┌──────────────────────────┐                      ║
  ║                 │   MerkleAccumulator      │  O(log n) memory     ║
  ║                 └────────────┬─────────────┘                      ║
  ║                              ▼                                    ║
  ║                     ┌──────────────────┐                          ║
  ║                     │ VerifReceipt     │                          ║
  ║                     │  table_root      │                          ║
  ║                     │  column_order[]  │                          ║
  ║                     │  key_columns[]   │                          ║
  ║                     │  total_rows      │                          ║
  ║                     └────────┬─────────┘                          ║
  ╚══════════════════════════════╪════════════════════════════════════╝
                                 │ state dir
  ╔══════════════════════════════╪════════════════════════════════════╗
  ║  verify                      ▼                                    ║
  ║  ┌────────────┐  rows[]  ┌──────────────────┐                     ║
  ║  │ Destination│─────────▶│ stage_destination│                     ║
  ║  │  (re-read) │          │  hash + key      │                     ║
  ║  └────────────┘          └────────┬─────────┘                     ║
  ║                                   ▼                               ║
  ║                 ┌──────────────────────────┐                      ║
  ║                 │ verifyhash log (staged)  │  sorted at seal      ║
  ║                 └────────────┬─────────────┘                      ║
  ║                              ▼                                    ║
  ║      rowhash  ──────▶ ┌────────────────────┐                      ║
  ║                       │   keyed set diff   │  one merge-join pass ║
  ║      verifyhash ────▶ │  → actual_root     │  O(1) memory         ║
  ║                       │  → missing/extra/  │                      ║
  ║                       │    changed by key  │                      ║
  ║                       └─────────┬──────────┘                      ║
  ║                                 ▼                                 ║
  ║                        VerificationResult                         ║
  ║          Match | Mismatch | NoPriorRun | LogUnavailable           ║
  ╚═══════════════════════════════════════════════════════════════════╝
```

---

## The Receipt

One receipt per destination table per pipeline, written to the state store at
`receipt:{pipeline_name}:{table_name}`, a key that is stable across runs, so each
`apply --integrity` overwrites the previous one and `verify` always compares
against the most recent migration.

It records:

- **`table_root`** - the Merkle root over every row leaf, taken in ascending
  row-key order. This is the commitment; everything else exists to make it
  reproducible.
- **`column_order`** - the destination columns, lexicographically sorted, that
  were fed to the hasher.
- **`key_columns`** - the destination key columns, in table order. Empty means the
  table had no primary key and each row hash served as its own key.
- **`total_rows`** - distinct row keys committed to the root, i.e. the tree's leaf
  count. **`skipped_rows`** - rows sent to the DLQ, so an expected absence is not
  read as data loss.
- **`algorithm`**, **`run_id`**, **`created_at`** - which hash function produced
  the root, and which run.

The column order and key columns are embedded rather than re-derived at verify
time. Introspecting the destination again would let a schema change between apply
and verify silently alter the encoding, which would look like a data mismatch.

The per-row `(key, hash)` set the root commits to is **not** in the receipt; it
lives beside it in the row-hash log (`rowhash/baseline/…`). The receipt stays a fixed
~200 bytes whatever the table size.

The receipt is yours to read back at any time: `pag receipt -c migration.ppl`
prints every stored receipt for the config with its full root, and `--json`
exports them as text you can commit or attach (see
[output-modes.md](output-modes.md#receipt-output) for the exact shapes).

## Reading a Result

`verify` returns one result per (pipeline, table):

- **Match** - every key present on both sides with identical contents, and the
  recomputed root equals the receipt's.
- **Mismatch** - carries the recomputed root plus complete counts of `missing`
  (in the receipt, gone from the destination), `extra` (in the destination, never
  migrated), and `changed` (same key, different contents), alongside the expected
  and actual row counts.
- **NoPriorRun** - no receipt for this pipeline and table; the migration ran
  without `--integrity`. Not an error.
- **LogUnavailable** - a receipt exists, but the row-hash log it commits to is
  missing or truncated (cleared by hand, or `verify` run against a different
  state directory than the one `apply` wrote to). The destination cannot be
  diffed against the committed set, so the result is **inconclusive**: `verify`
  exits 4 rather than reporting every intact row as `extra`. Re-run
  `apply --integrity` to rebuild the log.

A mismatch also lists individual diverging rows by key (`actor_id=42`), each
tagged missing / extra / changed. The counts are always complete; only that
detail list is capped, at 100 rows, so a fully-diverged table cannot produce an
unbounded report.

`verify` exits 0 when every table with a receipt matched, 3 on any mismatch, 4
when a table is inconclusive and none mismatched, and 1 on an error; see
[output-modes.md](output-modes.md#default-output) for the table.

---

## Canonical Row Serialization

The same row must always produce the same byte sequence regardless of whether it is read from the producer transform output or from a destination `SELECT`. `RowHasher` in `model` implements it, behind a single entry point shared by the apply and verify paths; there is deliberately no second encoder that could drift out of sync with the first.

### Encoding Protocol

For a given row, using `column_order` from `VerificationReceipt`:

```
for each column_name in column_order (lexicographic order):
    look up field in record by name
    if field not found: treat as Null

    write 1-byte type tag + value bytes
```

| Type | Tag | Encoding |
| --- | --- | --- |
| `Null` / missing | `0x00` | no body |
| `Int(i)` | `0x01` | 8-byte little-endian i64 |
| `UInt(u)` | `0x02` | 8-byte little-endian u64 (values ≤ i64::MAX normalize to `0x01`) |
| `Boolean(b)` | `0x03` | normalized to `0x01` Int(0/1) for cross-engine parity |
| `String(s)` | `0x10` | 4-byte LE length + UTF-8 bytes |
| `Decimal(d)` | `0x11` | form byte `0x00` + 16-byte LE mantissa + 8-byte LE exponent, trailing zeros stripped (so `1.50` = `1.5`, and `0.00` = `0`); mantissas over i128 fall back to form byte `0x01` + length-prefixed normalized text |
| `Float(f)` | `0x12` | 8-byte big-endian IEEE 754 (NaN -> `0x00` Null tag) |
| `Date(d)` | `0x20` | 4-byte LE signed days since Unix epoch |
| `Timestamp(ts)` | `0x21` | 8-byte LE microseconds since Unix epoch, UTC |
| `Uuid(u)` | `0x30` | 16 bytes big-endian |
| `Binary(b)` | `0x40` | 4-byte LE length + raw bytes (valid UTF-8 normalizes to `0x10`) |
| `Json(j)` | `0x50` | canonical JSON (sorted keys), 4-byte LE length prefix |
| `Array(a)` | `0x60` | 4-byte LE element count + recursively encoded elements |
| `Enum { value }` | `0x70` | string value only - no type name |

Every encoding is self-delimiting, so a stored key can be decoded back into readable text (`actor_id=42`) when a divergence is reported; nothing human-readable has to be stored per row.

### Column Order

`column_order` is the list of destination column names sorted lexicographically, established at migration start and stored in the receipt. Both the write path and verify path use this exact list. This decouples the hash from result-set ordering, which varies by driver.

### Value Coercions

A value can be stored by the destination in a different shape than it was handed over in, and verify re-reads the stored shape. Where that happens, the hash has to be taken over what the destination will store, not over what the pipeline produced:

- **A comma-joined string written to an array-like column** (a PostgreSQL array (`TEXT[]`) or a MySQL `SET`) is split into `Value::Array` before hashing. Both destinations store it as a collection and read it back as one (`Value::Array` from PostgreSQL, `Value::Set` from MySQL, which canonicalize identically), so hashing the string as written would never match the read-back.

This is a property of the destination column type, not of any one write path: it applies equally to PostgreSQL `COPY`, MySQL `LOAD DATA`, and plain `INSERT`.

Column types come from `IntegrityConfig.column_types` on the write path and from destination introspection on the verify path, and are applied identically on both.

### Special Cases

- **NaN float**: Encoded as `0x00` (Null). NaN has undefined equality semantics.
- **Missing column**: Treated as Null. Handles nullable columns absent from a record.
- **Timestamp timezones**: Normalized to UTC before encoding.
- **Enum type names**: Hashed by their string label, so equivalent values across a
  MySQL `ENUM` and a PostgreSQL enum produce identical bytes regardless of the type name.

---

## Row Keys

A row's key is the canonical encoding of its destination primary-key columns, in table order, using the same encoder and the same coercions as the row hash. A key computed from a transformed output row and a key computed from the same row read back out of the destination must be byte-identical even when the database normalizes the stored representation.

Two properties follow:

- **The key is independent of the payload.** A tampered row keeps its key, so it is reported as one `changed` row rather than as a missing row plus an unrelated extra one.
- **The key is independent of position.** Sorting by key gives a canonical order that has nothing to do with arrival order, which is what makes the Merkle root order-independent.

### Tables with no primary key

If the destination table has no primary key there is no meaningful key, so the row hash stands in as its own key and a warning is logged at apply time. Verification still works, with one documented limitation: byte-identical rows collapse into a single leaf, so such a table cannot distinguish "three identical rows" from "one". Tables with a primary key are unaffected.

---

## Merkle Tree Construction

### Leaves

```
leaf = H(0x00 || u32_le(len(key)) || key || row_hash)
node = H(0x01 || left || right)
```

Leaves and internal nodes are hashed in separate domains (RFC 6962 style), so a chosen row hash can never be substituted for an interior node, the general Merkle second-preimage attack. The key is bound into the leaf and length-prefixed, so moving a row to a different key changes the root and adjacent key/hash pairs cannot be re-cut to collide.

### Streaming fold

`MerkleAccumulator` combines two subtrees as soon as they have equal height, keeping at most one partial subtree per level:

```
Row leaves (ascending key order): l0  l1  l2  l3  l4
                                   \  /    \  /    ↑ promoted (odd node)
                                    b0      b1    l4
                                      \    /    /
                                       c0      c1
                                         \    /
                                          Root
```

Memory is O(log n): a 10M-row table holds ~24 pending hashes, not 10M. The resulting tree shape is identical to a level-by-level build that promotes an odd trailing node unchanged; a unit test asserts the two agree for every leaf count from 0 to 65.

### Order independence

There is exactly one tree per table, built over the store's key order, with no batch subtrees, per-lane roots, or sorted/unsorted variants. The root is a pure function of the set of `(key, row_hash)` pairs.

---

## State Persistence

### Receipt key

```
receipt:{pipeline_name}:{table_name}
```

Each `apply --integrity` overwrites the previous receipt for the same pipeline/table pair, so `verify` always compares against the most recent migration.

### Row hashes

Row hashes are bulk data with a narrow access pattern: append one record per
migrated row, read the whole set back once in key order, delete it. Nothing ever
looks a single row up. They are therefore not kept in the key-value store, which
would charge per-record index memory for an index no one queries. They live in an
append-and-sort log:

```
~/.paganel/state/rowhash/{scope}/{pipeline}/{table}/
    pending.log     appended during the run, unsorted
    run-000.tmp     sorted chunks, only while sealing a set too large to sort in memory
    sorted.log      the sealed set: sorted by key, one record per key

scope = "baseline"  committed by a migration or `state import`, kept until it is replaced
      = "verify"    staged by verify, cleared when it finishes
```

A record is `key_len | order | hash | key`. See
[Storage footprint](#storage-footprint) for what that costs in practice.

The lifecycle has three steps:

- **Append.** Every lane writes into one log per table. Writes are unordered and
  never read back during the run, so appends are a buffered sequential write and
  nothing accumulates in memory.
- **Seal.** Once the pipeline finishes, the log is sorted by key and deduplicated
  by external merge sort: chunks are sorted in memory and spilled as sorted runs,
  then merged through a small heap. Chunks are independent, so they are sorted
  across threads; the sort budget is the total across the chunks held at once, so
  concurrency costs run count rather than memory. A set small enough for one
  chunk (most tables) is sorted in memory and written straight out, with no
  runs and no merge. Peak memory is the budget (64 MiB), whatever the table size.
- **Stream.** The sealed file is read in key order, once, by the Merkle fold and
  again by the verify diff. Leaf hashing dominates the fold and is independent
  per row, so it too is spread across threads, in blocks of a power-of-two
  number of leaves, because the tree pairs leaves by absolute position and a
  misaligned split would build a different tree.

`order` is what makes a repeated key deterministic. Records carry a monotonic
counter, and the merge keeps the highest for each key, so the value written last
wins. An interrupted run leaves a sealed file behind; the resumed run's seal folds
it back in as one more sorted input, ranked below anything written since, so the
two runs' rows combine into one complete set.

Apply-scope hashes are cleared at the start of a fresh run: the receipt overwrites
in place, so a key left over from a larger earlier run would otherwise read as a
missing row forever. Clearing is a directory removal, independent of row count.

### Storage footprint

Row hashes are streamed to disk rather than held in memory, so their cost is disk,
not RAM. Measured through the real log, 1M rows per key shape:

| Destination primary key | Bytes per row | 10M rows | 100M rows |
| --- | --- | --- | --- |
| Integer (`BIGINT`) | 51 | 510 MB | 5.1 GB |
| `UUID` | 59 | 590 MB | 5.9 GB |
| Composite `(INT, INT)` | 60 | 600 MB | 6.0 GB |

A record is a 2-byte key length, an 8-byte order, the 32-byte hash, and the
canonically encoded key, so the fixed 42 bytes dominate, and even a UUID key adds
under 20%.

Before running a large migration with `--integrity`:

- **The sealed set is retained**, not deleted at the end of the run. `verify` reads
  it, so it has to outlive `apply`. It is replaced the next time the same pipeline
  runs, and removed by `pag reset`. A 10M-row table with an integer key leaves
  ~510 MB on disk under `~/.paganel/state/rowhash/baseline/`.
- **Sealing needs headroom.** The pending log, the sorted runs, and the merged
  output all exist at once for part of the seal, so peak disk during finalize is up
  to ~3x the sealed size, about **1.5 GB** for that 10M-row table, falling back to
  510 MB once the runs are deleted.
- **Verify doubles it, briefly.** Verify stages the destination's own keyed set
  under `verify/` to compare against, so both sets exist while it runs, another
  ~510 MB for the table above, cleared when it finishes.

Per pipeline and per table: a cascade migration touching four tables keeps four
sets, sized by each table's row count.

### What storage has to provide

Receipts need a key-value store: small, updated in place, read by key.

Row hashes need almost nothing (sequential append, sequential read, delete) and
in particular no index and no random access. Keeping the two apart is what lets a
ten-million-row integrity run hold a flat ~30 MB rather than growing with the
table.

---

## Verification Process

### Entry point

`engine-verify` takes an execution plan and returns one result per
(pipeline, table) pair. It has no dependency on `engine-runtime`: verification
never goes through the migration machinery.

### Staging the destination

Verify reads the whole destination table with a keyset scan over its primary key (falling back to `OFFSET` when there is none), hashes and keys each row with the receipt's `column_order` / `key_columns`, and writes the pairs under the verify scope (`rowhash/verify/…`). Nothing about the migration's shape (batch sizes, lane splits, read order) is replayed.

### The diff

Both sides now live in the same store, sorted by the same canonical key encoding, so one merge-join pass produces everything:

```
walk expected (baseline scope) and actual (verify scope) in lockstep by key:
    key only in expected            -> missing
    key only in actual              -> extra,   fold into actual_root
    key in both, hashes differ      -> changed, fold into actual_root
    key in both, hashes equal       ->          fold into actual_root
```

`actual_root` is folded from the destination side as the walk proceeds, so equal roots and clean counts are two views of the same fact. The pass runs on a blocking thread and uses O(1) memory regardless of table size.

### Output

```
✓ migrate_actor/actor - match (200 rows, root a3f1b2c49d8c7b6a, 45ms)
✗ migrate_payment/payment - MISMATCH (1 missing, 1 changed, 0 extra; 16049 rows expected, 16048 found; 312ms)
  expected root a3f1b2c49d8c7b6a
  actual   root 9d8c7b6a1f2e3d4c
  payment_id=3412 - changed: expected a3f1b2c49d8c7b6a actual 9d8c7b6a1f2e3d4c
  payment_id=9001 - missing from destination
? migrate_film/film - no integrity receipt (run `apply --integrity` first)
```

The CLI also supports writing the report to a file via `--output`, and printing
it as one JSON document via `--json` (see
[output-modes.md](output-modes.md#--json-output)).

---

## CLI Usage

```bash
# Run migration and commit a keyed Merkle receipt per destination table
pag apply -c migration.ppl --integrity

# Verify the destination against the stored receipt
pag verify -c migration.ppl

# Write the verification report to a file
pag verify -c migration.ppl --output report.txt

# Verify a table that also holds rows this run did not write
pag verify -c migration.ppl --allow-extra

# Print the result as JSON (hex roots, one entry per table)
pag verify -c migration.ppl --json

# Print the stored receipts (full table roots) - or export them as JSON
pag receipt -c migration.ppl
pag receipt -c migration.ppl --json > receipts.json
```

Without `--integrity`, hashing is disabled and costs nothing. There is
no shallower mode: the keyed row set is what the root is folded from, so storing it
is not optional.

### Pagination is not required

Verification does not depend on reproducing the migration's read order, so a `paginate` block is not needed for correct results. It is a good idea for large tables (keyset pagination avoids deep `OFFSET` scans), but its absence can't cause a false mismatch.

---

## Cascade, Lanes, and Retries

Three situations produce rows that arrive out of key order, from more than one
writer, or more than once. None of them needs special handling: a row's identity
is its key, re-writing the same row is idempotent, and the root is folded from the
finished set rather than as rows stream past.

- **Cascade tables** (`with references { data = cascade }`) are populated in
  FK-join order, and a given row may be pulled in alongside many different source
  batches. Each row is stored under its own key, so a repeat write lands on the
  same entry: the set converges on one leaf per distinct row however many times
  it was seen, and each row remains individually identifiable in a report.

- **Parallel lanes** all write into the same per-table set. Lanes need no
  coordination and nothing has to be merged when they finish, because two lanes
  writing different rows touch different keys, and the root is only folded once
  every lane has stopped.

- **Batch retries** re-hash rows that are already stored. Both the key and the
  hash are derived from the row's content, so the entry is rewritten byte for byte
  as it was: a retry cannot duplicate a row or move the root.

---

## Edge Cases

| Scenario | Behavior |
| --- | --- |
| Empty table | No rows, no receipt. Verify reports `NoPriorRun` rather than comparing against an empty tree. |
| Single row | Single-leaf tree; the root is that leaf. |
| Pipeline with `where` filter | Receipt covers only the filtered rows. Verify compares the destination against the receipt - the filter is not re-applied. |
| `action = skip` validation | Skipped rows are neither written nor hashed. `skipped_rows` records the count. |
| Rows inserted after migration | Reported as `extra`, named by key. |
| Rows deleted after migration | Reported as `missing`, named by key. |
| Row modified after migration | Reported as `changed`, named by key, with both hashes. |
| `NoPriorRun` | Pipeline ran without `--integrity`. Not an error. |
| Receipt present, row-hash log missing/truncated | `LogUnavailable` - inconclusive, `verify` exits 4 rather than reporting a false mismatch. Re-run `apply --integrity`. |
| Destination table with no primary key | Rows key by their own hash; a warning is logged. Duplicate identical rows are indistinguishable. |
| Multiple pipelines (DAG) | Each pipeline verifies independently. Results are collected in execution order. |
| Many null columns | Null-heavy rows serialize compactly (`0x00` per null column). |
| No `paginate` block | Fine. Read order does not affect the result. |

---

## Performance

### Write path overhead

| Operation | Cost | Notes |
| --- | --- | --- |
| Canonical serialization | ~200 ns/row | Buffer reuse via `RowHasher`, column positions resolved once per batch. |
| SHA-256 hash | ~300 ns/row | SHA-NI hardware acceleration, ~3 GB/s. |
| Row key encoding | ~20 ns/row | A few bytes for a typical integer PK. |
| Row-hash append | one buffered sequential write per batch per table | ~47 bytes per row, to disk. |
| Seal (once per table) | one external sort pass, chunks sorted in parallel | ~500 MB read + written at 10M rows. |
| Merkle fold (once per table) | ~2 hashes per row, leaves hashed in parallel | Streams the sealed file, O(log n) memory. |

End to end, `--integrity` adds roughly **0.3-0.5 µs per row**, near enough
constant across workloads: 15% on a plain 10M-row copy, up to 23% on the fastest
workload measured, and nothing measurable on one already bound by expression
evaluation. Full per-workload numbers are in
[benchmarks.md](benchmarks.md#the-cost-of---integrity).

The per-row costs above are rough, order-of-magnitude figures to show *where*
the work goes, not a benchmark. Hashing runs in-flight, overlapped with the
destination write rather than added on top of it. For measured numbers on real
hardware see [benchmarks.md](benchmarks.md).

### Memory

Two footprints, both flat.

**The engine's:** It holds one batch of `(key, hash)` pairs at a time; the
Merkle fold keeps one partial subtree per level (~24 hashes at 10M rows); the
verify diff walks both streams with one entry from each in hand. None of it grows
with the table.

**The storage layer's,** measured end to end through the real API (append, seal,
stream, clear):

| Row hashes | Resident during append | Peak (during seal) |
| --- | --- | --- |
| 3M | 28 MB | 73 MB |
| 10M | 28 MB | 73 MB |

The append phase does not grow at all, and the peak is the sort budget rather
than a function of the row count. Disk is the resource that scales instead; see
[Storage footprint](#storage-footprint).

### Verification path cost

Verify re-reads the destination in full, hashes and keys every row, stages the
result, seals it, and merge-joins the two streams. Measured on the same 10M-row
`orders` table used for the [benchmarks](benchmarks.md):

```
✓ orders/orders - match (10000000 rows, root 3e00a3e5b1cfedec, 18567ms)
```

**18.6 s, ~540k rows/s**, about 93% of the rate `apply` sustained writing the
same table (579k rows/s). That is the shape to expect: verification is a
sequential read of the destination plus a hash per row, so it costs roughly what
the migration cost, not a multiple of it.

Memory stays flat: the staging pass and the diff both stream. Disk does not:
staging writes a second copy of the keyed set, so the footprint in
[Storage footprint](#storage-footprint) roughly doubles while verify runs, and
the staged copy is deleted when it finishes.

---

## Future Extensions

### Source-side commitment

Take the same keyed commitment on the source as it is read, and the comparison becomes end-to-end: destination set ≡ f(source set) for the declared transform, rather than destination ≡ what the producer says it wrote. This is what would turn a tamper-evident receipt into a proof of migration correctness, and it is the natural home for excluding non-deterministic columns from the comparison.

### Inclusion proofs

The tree already supports it: an O(log n) proof that a specific row, with specific contents, was part of the migration, checkable against the published root without access to the rest of the table.

### Incremental verification

For CDC pipelines: update the committed set as rows arrive and re-fold only the affected path, extending the model from batch migrations to continuous replication.
