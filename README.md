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

## Storage Model

Ingest uses mutable COO buffers:

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

Fixed and variable double blocks are returned as fresh `double[]` values from public block accessors.

## Duplicate Policies

Set `:duplicate-policy` in a layout:

- `:last`, default
- `:sum`
- `:merge`, requires `:merge-fn`
- `:error`

`deps.edn` includes a test alias:

```sh
clojure -M:test
```
