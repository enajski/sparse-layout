(ns sparse-layout.flatiron-compare
  "Benchmark adapter for Flatiron's scalar-weight graph CSR plus block payloads."
  (:require [flatiron.graph :as graph]
            [sparse-layout.csr-source :as csr])
  (:import [flatiron.column F64Column I64Column]))

(set! *warn-on-reflection* true)

(defn from-source
  "Builds Flatiron's forward/reverse CSR and an owned block sidecar. Weights
   hold sidecar entry IDs, not payload values. Setup is outside serving timing."
  [source]
  (let [n
        (long (csr/csr-entry-count source))

        rows
        (long-array n)

        cols
        (long-array n)

        ids
        (double-array n)

        dim
        (long (csr/csr-block-dim source))

        values
        (double-array (* n dim))

        cursor
        (int-array 1)]

    (csr/csr-scan-ranges! source
                          [[0 (csr/csr-row-count source)]]
                          (fn [row col entry src]
                            (let [i (aget cursor 0)]
                              (aset rows i (long row))
                              (aset cols i (long col))
                              (aset ids i (double i))
                              (csr/csr-copy-block! src entry values (* i dim))
                              (aset cursor 0 (inc i)))))
    {:graph (graph/graph (I64Column. rows n 0 false nil)
                         (I64Column. cols n 0 false nil)
                         (F64Column. ids n 0 false nil)
                         (max (csr/csr-row-count source) (csr/csr-col-count source)))
     :values values
     :block-dim dim}))

(defn runner
  "Copies forward CSR rows and their sidecar blocks into preallocated arrays.
   Uses Flatiron's primitive arrays directly, not its lazy neighbors API."
  [{:keys [graph values block-dim]} start end destination]
  (let [^longs offsets
        (:fwd-offsets graph)

        ^longs targets
        (:fwd-targets graph)

        ^doubles ids
        (:weights graph)

        ^doubles payload
        values

        dim
        (long block-dim)

        start
        (long start)

        end
        (long end)

        ^ints rows
        (:rows destination)

        ^ints cols
        (:cols destination)

        ^doubles output
        (:values destination)]

    (fn []
      (loop [row
             start

             dst
             0]

        (if (< row end)
          (let [next-dst (loop [entry (aget offsets row)
                                dst dst]

                           (if (< entry (aget offsets (inc row)))
                             (do (aset rows dst (int row))
                                 (aset cols dst (int (aget targets entry)))
                                 (System/arraycopy payload
                                                   (* (long (aget ids entry)) dim)
                                                   output
                                                   (* dst dim)
                                                   dim)
                                 (recur (inc entry) (inc dst)))
                             dst))]
            (recur (inc row) (long next-dst)))
          dst)))))
