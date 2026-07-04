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

The source layer can also persist fixed-double-block CSR sources to a language-neutral mmap artifact:

```clojure
(csr/write-csr-artifact! source "/tmp/features.slcsr")
(def mmap-source (csr/open-csr-artifact "/tmp/features.slcsr"))

(csr/csr-copy-block! mmap-source 0 out 0)
```

The v1 artifact is a single little-endian binary file with a fixed header, section table, primitive CSR sections, payload doubles, and typed row/column key dictionaries. Readers validate the section table, CSR row pointers, column ids, and key dictionaries on open, throwing `ex-info` for corrupt artifacts. They rebuild key lookup maps on open while keeping row pointers, column ids, and payload values mmap-backed. The current JVM reader maps the artifact as one `ByteBuffer`, so files must be smaller than `Integer/MAX_VALUE`; the mapping is GC-managed and remains live while the returned source is reachable.

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

`deps.edn` includes a test alias:

```sh
clojure -M:test
```

Use `bb bridge run-evidence --id unit` or just `bb bridge auto` to run all tests and update evidences.

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
