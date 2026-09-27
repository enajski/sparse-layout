# sparse-layout

A small Clojure sparse layout compiler prototype.

`defsparse` turns a declarative nested-data shape into a mutable COO ingest builder and a frozen primitive-array-backed sparse datastore with CSR and/or CSC indexes.

```clojure
(require '[sparse-layout.core :refer [defsparse]])

(defsparse entity-features
  {:row-key [:entity]
   :cols-path [:vals]
   :payload {:kind :fixed-double-block
             :dim 3}
   :indices #{:csr :csc}})

(def ds
  (entity-features-compile
   [{:entity 1
     :vals (array-map :f1 [1.0 2.0 3.0]
                      :f2 [4.0 5.0 6.0])}
    {:entity 2
     :vals (array-map :f2 [7.0 8.0 9.0])}]))

(vec (seq (entity-features-block ds 1 :f1)))
;; => [1.0 2.0 3.0]

(entity-features-row ds 1)
;; => [[:f1 #double [...]] [:f2 #double [...]]]

(entity-features-col ds :f2)
;; => [[1 #double [...]] [2 #double [...]]]
```

## What This Is For

Use `sparse-layout` when the logical data model looks like a nested map:

```clojure
{row-key {col-key payload}}
```

but most row/column combinations are absent and hot paths need less allocation than ordinary persistent maps can provide. Typical examples include:

- sparse feature stores keyed by entity and feature name
- recommendation or scoring matrices keyed by user/item, entity/attribute, or document/token
- graph adjacency where rows represent source nodes and columns represent destination nodes
- event, metric, or observation tables where each entity has only a small subset of possible datapoints
- fixed-width numerical feature blocks where returning a zero-copy view is cheaper than allocating vectors

The library keeps the input shape declarative while compiling the frozen representation into primitive arrays and dictionaries. You ingest from nested Clojure records, then query by row, by column, or by point coordinate depending on which indexes were compiled.

## COO, CSR, and CSC

`sparse-layout` uses three sparse-matrix layouts for different phases and access patterns.

**COO, coordinate list**, is the ingest format. Each appended value is stored as a `(row-id, col-id, payload)` edge. COO is simple and append-friendly, which makes it a good fit while walking arbitrary nested Clojure data and dictionary-encoding row and column keys. It is not the final query format: freeze sorts COO edges, coalesces duplicates, compacts payload storage, and builds compressed indexes.

**CSR, compressed sparse row**, is the row index. It stores one pointer range per row plus the column ids and payloads for that row. CSR is the default because many sparse workloads are row-oriented: "give me all features for entity E", "walk outgoing graph edges", "score one example", or "read this row and one column inside it." Point lookups can use CSR by binary-searching within a row.

**CSC, compressed sparse column**, is the column index. It stores one pointer range per column plus row ids and payload ids. Compile CSC when reverse lookups are hot: "find all entities with feature F", "walk incoming graph edges", "aggregate one metric across entities", or "read all rows that have column C." CSC costs extra arrays at freeze time, so omit it when the workload never traverses columns.

Index choices are part of the layout:

```clojure
{:indices #{:csr}}       ;; row traversal and point lookup
{:indices #{:csr :csc}}  ;; row traversal, column traversal, and point lookup
```

By default, frozen datasets also retain the sorted COO coordinate arrays. Set `:retain-coo? false` when CSR/CSC are enough and the extra coordinate arrays are not needed.

## Generated API

For `(defsparse entity-features ...)`, the macro emits:

- `entity-features-layout`
- `SparseEntityFeatures`, a layout-specific marker protocol
- `EntityFeatures`, a frozen `deftype` holding dictionaries, edge arrays, indexes, and payload arrays
- `make-entity-features-builder`
- `entity-features-ingest!`
- `entity-features-freeze!`
- `entity-features-compile`
- `entity-features-row-id`
- `entity-features-col-id`
- `entity-features-row`
- `entity-features-col`
- `entity-features-block`
- `entity-features-row-views`
- `entity-features-col-views`
- `entity-features-block-view`

All frozen types also implement the shared `sparse-layout.core/SparseDataset` protocol:

```clojure
(row-id ds 1)
(col-id ds :f2)
(row-blocks ds 1)
(col-blocks ds :f2)
(block ds 1 :f2)
```

Block payload layouts also implement the shared `sparse-layout.core/SparseBlockViews` protocol:

```clojure
(row-block-views ds 1)
(col-block-views ds :f2)
(block-view ds 1 :f2)
```

`block-view` returns a `DoubleBlockView` that points at the frozen `double[]` payload storage without allocating a fresh block array. Use:

```clojure
(sparse-layout.core/block-view-array view)
(sparse-layout.core/block-view-offset view)
(sparse-layout.core/block-view-length view)
(sparse-layout.core/block-view-value view 0)
(sparse-layout.core/block-view->vec view)
```

Zero-copy views are supported for:

- `{:kind :fixed-double-block :dim n}`
- `{:kind :var-double-block}`

Scalar and object payload layouts continue to use the existing copying `block` API.

## Persistent-Style Facades

The optional `sparse-layout.facade` namespace provides read-only Clojure collection ergonomics over frozen datasets:

```clojure
(require '[sparse-layout.facade :as facade])

(def m (facade/as-map ds))

(get m 1)
;; => row map facade

(vec (seq (get-in m [1 :f2])))
;; => [4.0 5.0 6.0]

(contains? m 1)
(contains? (get m 1) :f1)
(find m 1)
(count m)
(count (get m 1))
(reduce-kv (fn [acc row-key row] acc) nil m)
```

Facades are deliberately read-only. `assoc` is not supported on frozen sparse datasets; use a builder and re-freeze when data needs to change.

Column-oriented facades are available when a CSC index was compiled:

```clojure
(def c (facade/col-map ds :f2))
(get c 2)
(seq c)
```

For zero-copy block traversal, use view facades:

```clojure
(def vm (facade/as-view-map ds))
(sparse-layout.core/block-view-value (get-in vm [1 :f2]) 0)
```

## CSR Source Layer

The `sparse-layout.csr-source` namespace exposes a lower-level storage API for fixed double block datasets. It adapts a frozen dataset into a `CSRSource` so callers can scan CSR rows or query-shaped row ranges and copy payload blocks into caller-owned `double[]` buffers without allocating one block per entry.

```clojure
(require '[sparse-layout.csr-source :as csr])

(def source (csr/dataset->csr-source ds))
(def out (double-array (csr/csr-block-dim source)))

(csr/csr-col-count source)
;; => 12

(csr/csr-copy-block! source 0 out 0)
```

When the consumer is filling primitive arenas, copy whole row ranges instead of
calling a Clojure function for every edge:

```clojure
(def ranges [{:row-start 0 :row-end 42}])
(def entry-capacity 512)
(def row-ids (int-array entry-capacity))
(def col-ids (int-array entry-capacity))
(def blocks (double-array (* entry-capacity (csr/csr-block-dim source))))

(csr/csr-copy-ranges! source ranges row-ids col-ids blocks 0)
;; => number of copied entries
```

`csr-copy-ranges!` validates all destination capacities before writing, preserves
CSR scan order, and supports both heap and mmap sources. `dst-entry-off` selects
the first row/column slot and the corresponding fixed block in `blocks`. Keep
`csr-scan-row!`/`csr-scan-ranges!` for custom per-edge logic and overlays; the
bulk copy is the low-allocation arena path.

`CSRSource` exposes explicit row, column, and entry counts. Row ids, column ids,
and entry ids are valid only inside their respective count ranges; heap and mmap
sources reject out-of-range ids instead of reading arbitrary storage.

`with-range-index` builds prefix ranges over an ordered row-key projection:

```clojure
(def ranged-source
  (csr/with-range-index source identity))

(csr/csr-resolve-ranges ranged-source {:prefix [:tenant-a :portfolio-1]})
;; => [{:row-start 0 :row-end 42}]
```

For mutable overlays, use `make-dok-delta-for-source` and an overlay view:

```clojure
(def delta (csr/make-dok-delta-for-source source))

(csr/delta-put! delta :row-a :col-b [1.0 2.0 3.0])
(csr/delta-delete! delta :row-a :col-c)

(def view (csr/overlay-view ranged-source delta))

(csr/scan-overlay-row! view :row-a visitor)
(csr/scan-overlay-selection! view {:prefix [:tenant-a :portfolio-1]} visitor)
```

Delta puts override main entries, deletes tombstone main entries, and delta-only rows with visible puts appear in logical overlay selections. Prefix overlay selections require a source prepared with `with-range-index`, so the view can reuse the same row projection for base and delta-only rows. Numeric merged range scans remain base-row-id scans and do not discover delta-only rows; use `scan-overlay-selection!` for user-facing mutable views. Use `make-dok-delta` directly only when you already have an explicit block dimension. Dimensioned deltas reject blocks whose length does not match the source block dimension.

Source-bound deltas cache each row in CSR column order and invalidate only the
row that changes. The overlay growth benchmark measures value overrides at
`0/1/10/100/1k/10k/100k` entries, with clustered and scattered row locality,
against bulk-clean-row splitting and full compaction:

```sh
clojure -M:overlay-compare smoke
clojure -J-Xmx6g -M:overlay-compare
```

The full run uses 262,144 base edges with 295 doubles per edge. Compaction and
split-plan construction are reported separately from repeated reads, and every
exact contender is checked against the current overlay output before timing.

The source layer can also persist fixed-double-block CSR sources to a language-neutral mmap artifact:

```clojure
(csr/write-csr-artifact! source "/tmp/features.slcsr")
(def mmap-source (csr/open-csr-artifact "/tmp/features.slcsr"))

(csr/csr-copy-block! mmap-source 0 out 0)
```

The v1 artifact is a single little-endian binary file with a fixed header, section table, primitive CSR sections, payload doubles, and typed row/column key dictionaries. Readers validate the section table, CSR row pointers, column ids, and key dictionaries on open, throwing `ex-info` for corrupt artifacts. They rebuild key lookup maps on open while keeping row pointers, column ids, and payload values mmap-backed. The current JVM reader maps the artifact as one `ByteBuffer`, so files must be smaller than `Integer/MAX_VALUE`; the mapping is GC-managed and remains live while the returned source is reachable.

### CSR64 MemorySegment API

Java 25 callers can use `sparse-layout.csr64` when the payload exceeds the v1
single-`ByteBuffer` limit. The v2 numeric artifact stores 64-bit row pointers,
32-bit column ids, and fixed-width doubles in separate file-backed
`MemorySegment` regions. It deliberately omits key dictionaries and prefix
indexes: logical keys must be resolved to snapshot-local numeric ids before the
hot call. Files are little-endian on every host, and reading them requires a
little-endian host (x86-64, ARM64): `open-artifact` throws `ex-info` on a
big-endian JVM before mapping anything.

The repository includes an SDKMAN environment file for Amazon Corretto 25:

```sh
sdk env
```

Write and open a v2 artifact from any existing `CSRSource`, then drain a row
range page by page:

```clojure
(require '[sparse-layout.csr64 :as csr64])

(csr64/write-artifact! source "/tmp/features.slcsr64")

(defn lane-0-sum
  "Sums lane 0 over the entries of one page."
  ^double [page]
  (let [n (count page)]
    (loop [i 0 sum 0.0]
      (if (< i n) (recur (inc i) (+ sum (csr64/page-lane page i 0))) sum))))

(with-open [mapped (csr64/open-artifact "/tmp/features.slcsr64")]
  ;; One reusable buffer: int[1024] row and column ids and
  ;; double[1024 × block-dim] values.
  (let [buf (csr64/page mapped 1024)]
    (transduce (map lane-0-sum) + 0.0 (csr64/pages mapped 0 64 buf))))
```

`pages` refills the same buffer on every step, so a page is valid only inside
the step that receives it: summarize it there, or copy out what you keep.
`(count page)` is the number of entries in the step. `page-row`, `page-col` and
`page-lane` read one entry with index checks; `page-rows`, `page-cols` and
`page-values` return the backing arrays for bulk hand-off, of which only the
first `count` entries (`count × block-dim` values) are valid. `pages`
implements `IReduceInit` only: `reduce`, `transduce` and `into` with a
transducer work, while `seq` throws. Returning `reduced` stops the drain before
the next copy. `row-count`, `col-count`, `entry-count` and `block-dim` return
the shape as primitive longs, and printing a source or a page shows only its
path and shape.

`reduce-entries` reads the mapping entry by entry, without a page buffer. When a
reduction needs only a few lanes per entry, read them with `lane`:

```clojure
(defn weighted-lane-0
  "Sums lane 0 over rows [row-start, row-end), weighted by column."
  ^double [mapped ^doubles weights row-start row-end]
  (csr64/reduce-entries [_row col e] [mapped row-start row-end] [acc 0.0]
    (+ acc (* (aget weights col) (csr64/lane mapped e 0)))))
```

To copy whole blocks, call `csr/csr-copy-block!` once per entry instead of
`lane` once per double:

```clojure
(defn copy-entries!
  "Copies the entries of rows [row-start, row-end) into caller-owned arrays
  sized for them; returns the number of entries."
  [mapped ^ints row-ids ^ints col-ids ^doubles values row-start row-end]
  (let [dim (csr64/block-dim mapped)]
    (csr64/reduce-entries [row col e] [mapped row-start row-end] [slot 0]
      (aset row-ids slot (int row))
      (aset col-ids slot (int col))
      (csr/csr-copy-block! mapped e values (* slot dim))
      (inc slot))))
```

`reduce-entries` binds `row`, `col` and the entry id `e` as primitive longs, in
`csr-scan-ranges!` order, and expands to one flat loop over the entries of the
range; `[mapped]` alone covers every row. `lane` reads lane `k` of an entry with
index checks. A direct `lane` call expands at compile time into that checked
read, so redefining `lane` does not reach callers compiled earlier; `apply` and
higher-order use still call the function. The source, the bounds and `init` are
evaluated once, and an invalid row range throws `ex-info`. A primitive `init`
and body keep `acc` unboxed, so the loop allocates nothing per entry; any other
body still works, but boxes. The reduction borrows the source rather than a
buffer: the bound values are plain numbers, but the source must stay open until
the call returns, and reading a closed source throws the JDK's
`IllegalStateException`. The body sits in expression position, where Clojure
compiles an inner `loop` or `dotimes` into a closure that allocates and boxes:
put multi-lane work in a primitive-typed helper such as
`(defn dot ^double [src ^long e ^doubles w] …)` and call it from the body.
`reduced` does not stop the loop; use `pages` to stop early.

`copy-page!` is the primitive under `pages`, for callers owning their arrays. It
counts the selected range before writing, validates every destination capacity,
and copies at most `max-entries`. It returns the number of copied entries; add
that to the cursor for the next page. An empty range or an exhausted cursor
returns `0` without writing, as the compiled overlay does.
`find-entry` and `copy-point!` provide numeric point lookup. `copy-point!`, here
and on compiled overlays, returns `dst`, or nil when the coordinate is absent.
Without `dst-off` it copies to offset 0; on a mapped source this arity takes
primitive `long` ids, so, unlike the five-argument arity, it does not box them.
`load!` requests best-effort residency from the operating system, while
`loaded?` exposes the corresponding point-in-time hint.

Rows must contain strictly increasing, unique column IDs. The current opener
checks column bounds but does not establish that ordering; malformed artifacts
can produce incorrect point and overlay results. Write new generations to fresh,
unpublished paths: the writer truncates an existing target. Keep a generation
alive across every page, with distinct row/column arrays owned by the caller.

Run the matched heap/CSR64 benchmark with:

```sh
clojure -M:csr64-bench
clojure -M:csr64-bench ceiling
```

The local ARM64 macOS run on Corretto 25.0.4 used 100 samples per selection and
reported:

| Shape / backend | Selection | Payload | median | p99 | allocation/call |
|---|---:|---:|---:|---:|---:|
| medium / CSR64 | 262,144 entries | 590 MiB | 19.360 ms | 20.788 ms | 192 B |
| medium / heap CSR | 262,144 entries | 590 MiB | 18.621 ms | 21.711 ms | 400 B |
| ceiling / CSR64 | one row, 255 entries | 588 KiB | 0.018 ms | 0.023 ms | 216 B |
| ceiling / CSR64 | 64 rows, 16,320 entries | 36.7 MiB | 1.301 ms | 1.534 ms | 216 B |
| ceiling / CSR64 | final 226,695 entries | 510 MiB | 16.546 ms | 18.061 ms | 216 B |

The ceiling artifact contained 2,147,448,075 doubles across 7,279,485 entries:
15.9997 GiB of payload and 16.0271 GiB on disk. It wrote in 6.254 seconds,
opened and structurally validated in 98.6 ms, and preloaded in 1.501 seconds.
These warm-cache p99 values prove the local implementation gate, not a cloud
tail-latency guarantee. The benchmark intentionally pages the result rather
than allocate or copy one maximum-length output array.

### CSR64 modification-ledger overlay

`sparse-layout.csr64-overlay` compiles a consistent, ordered modification-ledger
cut into an immutable overlay. Ledger records use numeric snapshot-local IDs and
strictly increasing sequence numbers:

```clojure
(require '[sparse-layout.csr64-overlay :as overlay])

(def view
  (overlay/compile-ledger
    mapped
    [{:sequence 41
      :op :put
      :row-id 7
      :col-id 3
      :block (double-array 295)}
     {:sequence 42 :op :delete :row-id 9 :col-id 5}]))

(overlay/copy-page! view 0 64 0 capacity rows cols values 0)
```

The latest record for a coordinate wins. Existing-value puts stay on a fast
lane: CSR64 bulk-copies the base page, then patches destination blocks by base
entry ID. Only rows containing an insert or effective delete use a sorted row
merge. The compiled view owns copies of put blocks, is safe for concurrent
readers, and requires its mapped base to remain open. Ledger durability and
transaction boundaries remain the caller's responsibility.

Run the medium, 295-double overlay benchmark with:

```sh
sdk env
clojure -M:csr64-bench overlay
```

| Ledger cut over a 590 MiB base | Selection | median | p99 | allocation/call |
|---|---:|---:|---:|---:|
| 100,000 value replacements | one row | 0.025 ms | 0.080 ms | 1,736 B |
| 100,000 value replacements | 64 rows | 0.140 ms | 0.324 ms | 23,192 B |
| 100,000 value replacements | full | 42.439 ms | 46.555 ms | 5,128 B |
| 1,000 structural rows | one row | 0.034 ms | 0.065 ms | 4,280 B |
| 1,000 structural rows | 64 rows | 0.310 ms | 0.898 ms | 156,128 B |
| 1,000 structural rows | full | 20.464 ms | 23.538 ms | 2,441,696 B |

All used 100 samples and passed the 50 ms p99 gate locally. The 100,000-value
case is close to the ceiling; compact to a new CSR64 generation before a larger
ledger cut or wider page is admitted. The full design and lifecycle are in
[`docs/csr64-modification-ledger-overlay.md`](docs/csr64-modification-ledger-overlay.md).

## Executable Documentation

This repo includes a Clerk notebook at `notebooks/sparse_layout/csr_source_notebook.clj` that walks through the CSR source layer end to end: logical records, frozen CSR internals, storage-level scans, query-shaped range indexes, mutable deltas, merged rows, and the mmap backend boundary.

Serve the notebook locally:

```bash
clojure -X:clerk
```

Then open `http://localhost:7777` and select the CSR source notebook. To build static HTML:

```bash
clojure -X:clerk:clerk/build
```

## Storage Model

Ingest starts with mutable COO buffers:

- row and column ids are dictionary-encoded with `java.util.HashMap`
- row ids and column ids are appended to growable primitive `int[]` buffers
- payloads are appended to payload-specific primitive buffers where possible

Freeze performs:

1. COO sort by row and column
2. duplicate coalescing according to `:duplicate-policy`
3. payload compaction into primitive arrays
4. CSR and/or CSC index compilation
5. construction of the generated frozen `deftype`

By default, the frozen form retains COO coordinate arrays alongside the compressed indexes:

```clojure
{:retain-coo? true}
```

If you want the frozen dataset to drop those extra coordinate arrays after freeze, set:

```clojure
{:retain-coo? false}
```

With `:retain-coo? false`, row traversal and point lookups use `:csr-row-ptrs` plus `:csr-col-ids`, and column traversal uses `:csc-col-ptrs`, `:csc-row-ids`, and `:csc-payload-ids`.

The frozen structure stores arrays equivalent to:

```clojure
{:edge-rows int-array
 :edge-cols int-array
 :csr-row-ptrs int-array
 :csr-col-ids int-array
 :csc-col-ptrs int-array
 :csc-row-ids int-array
 :csc-payload-ids int-array
 :payload-values primitive-or-object-array
 :payload-ptrs int-array}
```

## Payload Kinds

Supported payload specs:

- `:double`
- `:long`
- `:object`
- `{:kind :fixed-double-block :dim n}`
- `{:kind :var-double-block}`

Fixed and variable double blocks are returned as fresh `double[]` values from generated block accessors.

## Duplicate Policies

Set `:duplicate-policy` in a layout:

- `:last`, default
- `:sum`
- `:merge`, requires `:merge-fn`
- `:error`

## Verification

This project uses `bridge` to maintain verification-aware alignment between code, specs, tests, and evidence.

To analyze changes and check obligations:

```sh
bb bridge next
```

`deps.edn` includes test, lint, and advanced-format aliases. Use Java 25 through
the checked-in SDKMAN environment before running them:

```sh
sdk env
clojure -M:test
clojure -M:lint
clojure -M:format
clojure -M:format check
```

Use `bb bridge run-evidence --id unit`, `bb bridge run-evidence --id lint`, and
`bb bridge run-evidence --id format`, or just `bb bridge auto`, to refresh
required evidence.

It also includes a Criterium benchmark alias:

```sh
clojure -M:bench
```

The alias includes the JVM 17+ Criterium blackhole flags:

```sh
-XX:+UnlockExperimentalVMOptions
-XX:CompileCommand=blackhole,criterium.blackhole.Blackhole::consume
```

The benchmark suite builds deterministic flat `[row col payload]` entries, then constructs each compared representation from that same source:

- a frozen `sparse-layout` dataset with CSR and CSC indexes
- a nested row-only index, `row-key -> col-key -> payload-vector`
- a nested dual index with both `row-key -> col-key -> payload-vector` and `col-key -> row-key -> payload-vector`

It compares point lookup, row scans, column scans, row-only full column scans, and construction cost from the shared entry source. Benchmarks run for scalar row/column keys and for compound Clojure map row/column keys with several key-value pairs.

`clojure -M:bench` uses Criterium's quick benchmark mode for the standard small scalar and compound-map scenarios. Include larger dataset increments with:

```sh
clojure -M:bench large
```

Filter by key shape with:

```sh
clojure -M:bench scalar
clojure -M:bench compound
```

Filters compose, so `clojure -M:bench scalar large` runs all scalar-key increments. Use `medium` or `large-only` for a single larger scale.

Measure retained object graph sizes with the separate `:memory` alias. This alias uses `clj-memory-meter` and includes JVM flags for dynamic agent loading:

```sh
clojure -M:memory
clojure -M:memory scalar large
```

The memory report measures each root independently and prints total retained size plus bytes per non-zero entry for:

- the flat source entries
- the frozen sparse dataset with CSR and CSC
- the nested row-only index
- the nested dual row+column index

Use the longer Criterium mode with:

```sh
clojure -M:bench full
```

To compile the benchmark namespace and run each benchmarked operation once without Criterium timing, use:

```sh
clojure -M:bench smoke
```

### DuckDB / Parquet comparison

The matched serving benchmark pins DuckDB 1.5.5, uses a 295-double block per
`(row-key, col-key)` edge, and fills the same preallocated `int[]`/`double[]`
destinations through seven contenders:

- a preloaded Java 25 `MemorySegment` CSR64 artifact, read three ways: one
  `copy-page!` call (`csr64`), a `pages` drain through a reusable 4,096-entry
  `page` copied out page by page (`csr64-pages`), and `reduce-entries` copying
  each block with one `csr-copy-block!` call (`csr64-reduce-entries`);
- `csr-copy-ranges!` on a Parquet-derived `CSRSource`;
- the same source's generic per-edge visitor;
- Yogthos [Flatiron](https://github.com/yogthos/flatiron)'s graph CSR with a block sidecar;
- Parquet with 295 scalar columns;
- Parquet with 295 `(lane, value)` rows per edge; and
- one Parquet list cast back to DuckDB `DOUBLE[295]` before projection.

It verifies exact output equality before timing and reports Parquet generation,
Parquet → COO → CSR construction, CSR64 write/open/preload time, file size,
median/p95/p99 materialization time, the 50 ms result, and current-thread JVM
allocation. Each CSR64 row uses 100 measured samples; the slower controls use
5–30. The run exits nonzero only when the `csr64` row misses the 50 ms p99
ceiling; the other rows just report it. DuckDB native allocation is not included.

Flatiron is pinned to `fd27b76d6af097ab3211feffabda5d439ec6e4dd` in the
benchmark-only alias. `flatiron-csr-sidecar` uses its real `graph/graph`
constructor (including the reverse index), with scalar weights holding entry
IDs into an owned 295-double-per-edge sidecar. The timed adapter traverses
Flatiron's forward CSR arrays directly and copies full blocks, not just scalar
weights. CSR → Flatiron + sidecar setup time/allocation is reported separately;
this is not native Flatiron block storage or an independent Parquet ingest path.
Run adapter regression tests with `clojure -M:duckdb-compare:compare-test`.
The historical table below predates the Flatiron contender.

```sh
sdk env
clojure -M:duckdb-compare smoke
clojure -J-Xmx4g -M:duckdb-compare small compound
clojure -J-Xmx6g -M:duckdb-compare medium compound
clojure -J-Xmx12g -M:duckdb-compare large-only compound
```

The local Corretto 25 comparison reported:

| Scale / selection | CSR64 median / p99 | Best DuckDB median / observed p99 | DuckDB shape |
|---|---:|---:|---|
| small / one row | 0.006 / 0.010 ms | 1.953 / 2.010 ms | 295 rows |
| small / 64 rows | 0.074 / 0.103 ms | 6.809 / 6.874 ms | 295 rows |
| small / full, 55 MiB | 2.052 / 2.532 ms | 73.990 / 80.361 ms | 295 columns |
| medium / one row | 0.005 / 0.013 ms | 7.949 / 9.185 ms | 295 rows |
| medium / 64 rows | 0.079 / 0.105 ms | 15.251 / 15.449 ms | 295 rows |
| medium / full, 590 MiB | 18.558 / 21.463 ms | 783.775 / 821.306 ms | 295 columns |
| large / one row | 0.005 / 0.011 ms | 22.986 / 24.060 ms | 295 rows |
| large / 64 rows | 0.103 / 0.150 ms | 32.652 / 33.410 ms | 295 rows |
| large / full, 1.73 GiB | 58.262 / 62.020 ms | 2,380.886 / 2,382.805 ms | 295 columns |

CSR64 passes the 50 ms p99 ceiling through the 590 MiB full drain and for every
measured one-row/64-row selection. A single 1.73 GiB drain fails, so the large
command deliberately exits nonzero: that response must use bounded pages rather
than one maximum-sized destination operation. DuckDB's 295-row shape remains
under 50 ms for the selective cases but no DuckDB full drain does.

Apache Parquet has no fixed-size-list logical type. The compact source is
therefore read by DuckDB as `DOUBLE[]`; the benchmark casts it to
`DOUBLE[295]` to enforce the invariant. DuckDB 1.5.5's Java chunked-result API
does not expose nested values, so it projects the 295 primitive lanes before
copying them into the caller's array.

## Construction Benchmark

The `:construct` alias isolates the freeze step (COO → CSR/CSC) and reports
wall-clock mean plus per-call heap allocation for the shipped freeze path:

```sh
clojure -M:construct
clojure -M:construct scalar large
clojure -M:construct compound medium
```

This alias is intended as a local validation tool; long-form construction notes
are maintained in the project wiki rather than this repository.
