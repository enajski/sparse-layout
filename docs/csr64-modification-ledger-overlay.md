# CSR64 modification-ledger overlay

The changes described below as recommendations are not implemented yet.

## Decision

Keep the mapped CSR64 generation immutable. Persist changes in an ordered
modification ledger, then compile a finite ledger cut into an immutable,
row-aware serving overlay. Publish the base and compiled overlay together as one
generation.

```text
durable modification ledger ── replay through sequence S ──┐
                                                           ▼
mapped CSR64 generation ───────────────────────────► compiled overlay
                                                           │
query row range + cursor ──────────────────────────────────┤
                                                           ▼
                                                caller-owned arrays
```

The ledger is the change history. The compiled overlay is disposable query
state. A restart can reopen the base and rebuild the same overlay through the
same committed sequence.

## Contract

Each input record is a Clojure map:

```clojure
{:sequence 1042
 :op :put                         ; or :delete
 :row-id 17
 :col-id 8
 :block double-array}             ; required only by :put
```

- Sequence numbers are non-negative and strictly increasing in replay order.
- Records must be maps. **Known validation defect:** a `nil` or `false` record
  currently terminates replay without rejecting or applying subsequent records
  (review R4). The intended behavior is rejection of malformed input.
- The latest record for a `(row-id, col-id)` coordinate wins.
- Row and column IDs are numeric and must already belong to the base generation.
- A put may create a previously absent coordinate inside those ID domains.
- A delete of an absent coordinate is a no-op.
- Put blocks must exactly match the CSR64 fixed block dimension and are copied
  during compilation.
- The mapped base must remain open as long as any derived overlay is readable.
- Keep the same base and ledger cut throughout a paged read; a cursor belongs
  to that generation. Concurrent readers need separate destination arrays, and
  row/column destination arrays must be distinct.
- Base rows must contain strictly increasing, unique column IDs. The current
  CSR64 opener checks bounds but not that ordering. An unordered artifact can
  make a replacement look like an insertion; see review finding R1.

New logical row or column keys require the next base generation. The overlay
does not introduce another dictionary or ID-allocation policy.

## Derived representation

Compilation collapses ledger history to the latest effective record per
coordinate, sorts it by numeric row and column ID, and separates two paths:

| Path | Contents | Query operation |
|---|---|---|
| value replacements | puts for coordinates already present in the base, sorted by base entry ID | bulk-copy the CSR64 range, then overwrite matching destination blocks |
| structural rows | inserts and effective deletes, plus any replacements in the same row | two-finger merge that row's base columns with its sorted modifications |

The structural-row index also stores cumulative entry-count adjustments. A
range count is therefore:

```text
base entries in [row-start, row-end)
+ structural adjustment before row-end
- structural adjustment before row-start
```

Only structural rows are indexed by row. The overlay does not allocate a second
`row-count + 1` pointer array, so its index size follows modified state rather
than the potentially much larger base domain.

## Query execution

`copy-page!` preserves the CSR64 contract: half-open numeric row range, logical
entry cursor, maximum entry count, caller-owned `int[]`/`double[]` destinations,
and validation before the first write.

The overlay returns `0` for empty or exhausted pages. The base currently returns
`nil` in those cases (review R2), so this intended shared contract is not yet
fully consistent.

1. Compute the visible range count from base pointers plus cumulative structural
   adjustments.
2. Translate the cursor to a starting row with binary search over visible row
   prefixes.
3. Bulk-copy every run that contains no structural row using CSR64
   `MemorySegment.copy`.
4. Apply replacement blocks whose base entry IDs fall inside that copied run.
5. Merge only structural rows, preserving ascending column ID order.
6. Stop at the requested page boundary and return the copied entry count.

`copy-point!` checks the row patch first, then a replacement's base entry ID,
then falls through to the mapped base.

For `S` structural rows, `P` replacement entries, `D` selected structural rows,
and `B` copied base bytes, the intended hot-path shape is:

```text
range count       O(log S)
cursor location   O(log(row-count) × log S)
clean page        O(B) bulk copy + O(log P + selected replacements)
structural work   O(base entries in D + modifications in D)
```

These are per-call work shapes, not a page-size latency bound. The base row-ID
fill walks intervening rows, including empty ones. More significantly, the
current structural merge restarts at the beginning of its dirty row on every
page and skips up to `row-offset` entries again. Draining one row of degree `d`
in pages of `k` can therefore take Θ(d²/k) merge work. A review probe copied one
entry at cursor 99,999 while performing 100,001 column reads. The uniform
16-entry benchmark does not exercise this case.

Value-only overlays also still perform visible-prefix searches and allocate
row-span vectors. Having no structural rows does not currently bypass that
machinery; it only keeps the payload on the bulk-copy-plus-replacements path.

The common value-update path never routes every base entry through a Clojure
callback.

## Publication and concurrency

Compiled overlays are immutable. The application should publish an object that
contains both base and overlay through one `AtomicReference` swap:

```text
reader: acquire generation → copy one or more pages → release generation
writer: read committed ledger cut → compile → atomically publish
retire: close old mapped base only after its last reader releases it
```

An atomic swap publishes the reference; safe reader retention and reclamation
remain application responsibilities. The base writer uses `TRUNCATE_EXISTING`:
write only to a fresh, unpublished generation path, never over the live mapping.

Ledger appends and transaction boundaries are outside this library. Compilation
must receive a stable, committed sequence. It rejects reordered or duplicate
sequence numbers rather than guessing at transaction order.

## Compaction

Compaction creates a new immutable CSR64 generation from `base + overlay`, then
publishes its generation ID and ledger cutoff atomically. Only after that base is
durable may the older ledger prefix and mapped generation be retired.

Trigger compaction from measured costs, not a universal mutation count:

- p99 for the admitted maximum page approaches 50 ms;
- retained overlay bytes approach the cost of writing a new CSR64 generation;
- structural dirty-row density makes row merging dominate bulk copying; or
- replay/compile time violates recovery objectives.

The current implementation deliberately provides the compiled read overlay, not
a second persistence format or compaction coordinator. A compactor should stream
bounded overlay pages into the existing next-generation artifact workflow; it
should not mutate the mapped base in place.

`CSR64LedgerOverlay` does not implement `CSRSource`, so it cannot be passed
directly to `csr64/write-artifact!`. Compaction still needs an appropriate source
adapter or page-writing workflow and a publication coordinator. No such public
compaction operation is supplied today.

For repeated reads, compare measured compaction cost with
`remaining reads × (overlay read time − compacted read time)`. Include recovery,
retained memory, downstream compute and concurrent-reader tail latency in the
decision. A raw record count alone does not capture row degree, modification
locality, or the amount of retained state.

## Local evidence

The following table is the original local evidence; it is retained rather than
replaced by a different harness's timings.

Corretto 25.0.4, ARM64 macOS, a 590 MiB base, 295 doubles per entry, warm mapped
pages, and 100 samples per selection:

| Ledger cut | Compile | Selection | median | p99 | JVM allocation/call |
|---|---:|---:|---:|---:|---:|
| 100,000 existing-value puts | 342.5 ms | one row | 0.025 ms | 0.080 ms | 1,736 B |
| 100,000 existing-value puts | — | 64 rows | 0.140 ms | 0.324 ms | 23,192 B |
| 100,000 existing-value puts | — | full 590 MiB | 42.439 ms | 46.555 ms | 5,128 B |
| 1,000 delete+insert rows | 5.5 ms | one row | 0.034 ms | 0.065 ms | 4,280 B |
| 1,000 delete+insert rows | — | 64 rows | 0.310 ms | 0.898 ms | 156,128 B |
| 1,000 delete+insert rows | — | full 590 MiB | 20.464 ms | 23.538 ms | 2,441,696 B |

Every measured selection passed the 50 ms p99 ceiling in two runs. The worst
observed full p99 was 46.555 ms for 100,000 replacements and 23.763 ms for 1,000
structural rows. That makes the former a local compaction boundary, not a
universal production constant. Cold-cache behavior, concurrent readers, and
real modification distributions still require deployment-specific evidence.

Run the benchmark with:

```sh
sdk env
clojure -M:csr64-bench overlay
```

### 5 September review rerun and next experiments

On an M1 Max / 64 GB / Corretto 25.0.4, the existing benchmark again passed its
local gate: full-page replacements were 42.059 ms median / 44.430 ms empirical
p99, and structural rows were 20.282 / 23.062 ms. Compilation took 330.2 ms and
5.25 ms respectively. Allocation matched the table above. These 100-sample
tails and three-call warmup are diagnostics, not production p99 guarantees.

The accompanying base-only probe used sustained warmup and three fresh JVMs;
its full CSR64 page was within 2.5% of a raw payload-only copy. That leaves more
useful optimisation work in the overlay:

1. Compile seekable clean/patch runs in structural rows, then bulk-copy surviving
   base spans. Check early and late pages against skewed-row reference results.
2. Bypass visible-prefix row search when there are no structural rows. Reuse the
   base cursor and apply value replacements directly.
3. Compare compaction with packing replacement blocks in base-entry order and
   coalescing adjacent runs. At 100,000 replacements, 38.15% of the base entries
   are replaced and the blocks alone retain 236 MB. Copying the base and then
   overwriting adds 472 MB of logical read/write traffic per full drain.
4. If profiles justify it, avoid a second base point search on replacement
   misses and use primitive prefix access instead of row-span vectors.

Preserve defensive block copying. Copying only final winners during replay is
safe only when the input guarantees stable blocks; a streaming decoder may
reuse one mutable array between records.

## Rejected alternatives

- **Mutate mapped payload bytes:** breaks immutable-generation publication,
  recovery, and reader isolation.
- **Replay the ledger during every query:** query cost grows with history rather
  than visible state.
- **Put every modified row on the merge path:** needlessly loses the bulk-copy
  fast path for value-only changes.
- **Shard the base to isolate updates:** adds mappings and page-fault surfaces
  without removing same-volume I/O on one disk.
- **Add another overlay file format now:** the heap overlay should compact before
  it becomes large enough to justify its own mapped format.
