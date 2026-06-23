(ns sparse-layout.csr-source-notebook
  {:nextjournal.clerk/no-cache true}
  (:require [nextjournal.clerk :as clerk]
            [sparse-layout.core :refer [block-view-value
                                        defsparse]]
            [sparse-layout.csr-source :as csr])
  (:import [java.nio.file Paths]))

;; # sparse-layout: from logical records to query-shaped CSR
;;
;; This notebook is executable documentation for the storage path in
;; `sparse-layout`. It walks from a nested logical shape to a frozen CSR layout,
;; then layers a query-shaped range index and mutable delta over that immutable
;; main store.
;;
;; The central idea is simple: keep the public data model pleasant, but compile
;; the physical layout into primitive arrays so hot readers can scan rows and
;; copy fixed-width blocks without allocating a new block per entry.

(clerk/html
 [:div {:style {:padding "1.1rem 1.25rem"
                :border-radius "8px"}}
  [:h3 {:style {:margin "0 0 .35rem"}} "What this notebook demonstrates"]
  [:ol {:style {:margin "0"
                :padding-left "1.25rem"}}
   [:li "Compile nested Clojure records into primitive-array CSR."]
   [:li "Adapt the frozen dataset to the lower-level CSRSource protocol."]
   [:li "Resolve query prefixes into row ranges."]
   [:li "Overlay a mutable DOK delta on top of immutable CSR."]
   [:li "Scan merged rows in deterministic column order."]]])

;; ## 1. Declare a sparse physical shape
;;
;; `defsparse` describes the logical record shape and the target payload
;; representation. The example support namespace declares:
;;
;; ```clojure
;; {:row-key [:row]
;;  :cols-path [:features]
;;  :payload {:kind :fixed-double-block
;;            :dim 3}
;;  :indices #{:csr :csc}}
;; ```
;;
;; Each coordinate stores a three-wide double block:
;; `[value, confidence, freshness]`.

;; The rows below are intentionally ordered by `[tenant portfolio book]`.
;; That order is what later makes prefix ranges cheap to resolve.

(def ^:nextjournal.clerk/no-cache demo-state
  (do
    (eval
     '(defsparse portfolio-eval-features
        {:row-key [:row]
         :cols-path [:features]
         :payload {:kind :fixed-double-block
                   :dim 3}
         :indices #{:csr :csc}}))
    (let [records [{:row [:acme :core :fx]
                    :features (array-map :exposure [12.0 0.91 1.0]
                                         :risk [0.42 0.78 1.0]
                                         :liquidity [0.88 0.83 1.0])}
                   {:row [:acme :core :rates]
                    :features (array-map :exposure [8.0 0.86 2.0]
                                         :risk [0.31 0.74 2.0])}
                   {:row [:acme :growth :equity]
                    :features (array-map :exposure [19.0 0.88 1.0]
                                         :momentum [1.12 0.67 3.0])}
                   {:row [:globex :core :fx]
                    :features (array-map :exposure [15.0 0.89 1.0]
                                         :liquidity [0.73 0.70 2.0])}
                   {:row [:globex :income :credit]
                    :features (array-map :risk [0.57 0.81 1.0]
                                         :carry [0.19 0.76 2.0])}]
          compile! @(requiring-resolve
                     'sparse-layout.csr-source-notebook/portfolio-eval-features-compile)
          row-value @(requiring-resolve
                      'sparse-layout.csr-source-notebook/portfolio-eval-features-row)
          block-view @(requiring-resolve
                       'sparse-layout.csr-source-notebook/portfolio-eval-features-block-view)
          dataset (compile! records)]
      {:records records
       :dataset dataset
       :sample-row (row-value dataset [:acme :core :fx])
       :exposure-view (block-view dataset [:acme :core :fx] :exposure)
       :source (csr/dataset->csr-source dataset)})))

(def records (:records demo-state))

(def dataset (:dataset demo-state))

(clerk/table records)

;; ## 2. The generated API stays ergonomic
;;
;; Existing callers can keep using the generated row, column, point, and view
;; APIs. The new source layer is additive.

(def sample-row (:sample-row demo-state))

sample-row

(def exposure-view (:exposure-view demo-state))

[(block-view-value exposure-view 0)
 (block-view-value exposure-view 1)
 (block-view-value exposure-view 2)]

;; ## 3. The frozen shape is compact CSR plus dictionaries
;;
;; The source adapter exposes the important CSR facts without exposing generated
;; dataset classes to hot consumers.

(def source (:source demo-state))

(def physical-summary
  {:rows (csr/csr-row-count source)
   :entries (csr/csr-entry-count source)
   :payload-kind :fixed-double-block
   :block-dim (csr/csr-block-dim source)
   :payload-doubles (* (csr/csr-entry-count source)
                       (csr/csr-block-dim source))})

(clerk/table [physical-summary])

;; ## 4. Adapt the frozen dataset to CSRSource
;;
;; `CSRSource` is the storage boundary. Consumers ask for spans, column ids, and
;; block copies; they do not need to know whether the backing store is a heap
;; array today or an mmap artifact later.

(defn block-values
  [^doubles blocks block-dim offset]
  (mapv (fn [j]
          (aget blocks (+ offset j)))
        (range block-dim)))

(defn materialized-scan->table
  [{:keys [source rows row-keys cols col-keys entries origins blocks count]}]
  (let [block-dim (csr/csr-block-dim source)]
    (mapv (fn [i]
            (let [row-id (aget ^ints rows i)
                  col-id (aget ^ints cols i)]
              (cond-> {:row-id row-id
                       :row-key (if row-keys
                                  (aget ^objects row-keys i)
                                  (when-not (neg? row-id)
                                    (csr/csr-row-key-at source row-id)))
                       :entry-id (aget ^ints entries i)
                       :col-id col-id
                       :col-key (if col-keys
                                  (aget ^objects col-keys i)
                                  (csr/csr-col-key-at source col-id))
                       :block (block-values blocks block-dim (* i block-dim))}
                origins (assoc :origin (aget ^objects origins i)))))
          (range count))))

(defn materialize-source-row
  [source row-id]
  (let [[start end] (csr/csr-row-span source row-id)
        entry-count (- end start)
        block-dim (csr/csr-block-dim source)
        rows (int-array entry-count)
        cols (int-array entry-count)
        entries (int-array entry-count)
        blocks (double-array (* entry-count block-dim))
        cursor (int-array 1)]
    (csr/csr-scan-row!
     source
     row-id
     (fn [row-id* col-id entry-id src]
       (let [i (aget cursor 0)
             block-off (* i block-dim)]
         (aset-int rows i row-id*)
         (aset-int cols i col-id)
         (aset-int entries i entry-id)
         (csr/csr-copy-block! src entry-id blocks block-off)
         (aset-int cursor 0 (inc i)))))
    {:source source
     :rows rows
     :cols cols
     :entries entries
     :blocks blocks
     :count (aget cursor 0)}))

(def row-catalog
  (mapv (fn [row-id]
          (let [[start end] (csr/csr-row-span source row-id)]
            {:row-id row-id
             :row-key (csr/csr-row-key-at source row-id)
             :entry-start start
             :entry-end end
             :entries (- end start)}))
        (range (csr/csr-row-count source))))

(clerk/table row-catalog)

;; Scan one row through the storage-level API. The visitor writes ids into
;; preallocated `int[]` arrays and copies payloads into one caller-owned
;; contiguous `double[]`.

(def row-0-scan
  (materialize-source-row source 0))

(clerk/table (materialized-scan->table row-0-scan))

;; ## 5. Query-shaped row ranges
;;
;; A range index maps query prefixes to row ranges. With rows ordered by
;; `[tenant portfolio book]`, a tenant or tenant+portfolio query can resolve to
;; a small set of contiguous row spans.

(def ranged-source
  (csr/with-range-index source identity))

(def acme-core-ranges
  (csr/csr-resolve-ranges ranged-source {:prefix [:acme :core]}))

acme-core-ranges

(defn range-entry-count
  [source ranges]
  (reduce (fn [n row-range]
            (let [[row-start row-end] ((fn [r]
                                         (cond
                                           (map? r) [(:row-start r) (:row-end r)]
                                           :else r))
                                       row-range)]
              (+ n
                 (reduce (fn [row-n row-id]
                           (let [[start end] (csr/csr-row-span source row-id)]
                             (+ row-n (- end start))))
                         0
                         (range row-start row-end)))))
          0
          ranges))

(defn materialize-ranges
  [source ranges]
  (let [entry-count (range-entry-count source ranges)
        block-dim (csr/csr-block-dim source)
        rows (int-array entry-count)
        cols (int-array entry-count)
        entries (int-array entry-count)
        blocks (double-array (* entry-count block-dim))
        cursor (int-array 1)]
    (csr/csr-scan-ranges!
     source
     ranges
     (fn [row-id col-id entry-id src]
       (let [i (aget cursor 0)
             block-off (* i block-dim)]
         (aset-int rows i row-id)
         (aset-int cols i col-id)
         (aset-int entries i entry-id)
         (csr/csr-copy-block! src entry-id blocks block-off)
         (aset-int cursor 0 (inc i)))))
    {:source source
     :rows rows
     :cols cols
     :entries entries
     :blocks blocks
     :count (aget cursor 0)}))

(clerk/table
 (materialized-scan->table
  (materialize-ranges ranged-source acme-core-ranges)))

;; Non-contiguous projections are still correct. This projection indexes only
;; the book segment, so `:fx` appears under both tenants and returns two ranges.

(def by-book-source
  (csr/with-range-index source (fn [[_tenant _portfolio book]]
                                 [book])))

(csr/csr-resolve-ranges by-book-source {:prefix [:fx]})

;; ## 6. Mutable delta over immutable CSR
;;
;; The DOK delta records puts and deletes without rewriting the main CSR. During
;; merged scans:
;;
;; 1. A delta put overrides the main coordinate.
;; 2. A delta delete suppresses the main coordinate.
;; 3. Delta-only coordinates are emitted.
;; 4. Output remains deterministic by column id.

(def delta
  (doto (csr/make-dok-delta)
    (csr/delta-put! [:acme :core :fx] :risk [0.99 0.95 0.0])
    (csr/delta-delete! [:acme :core :fx] :liquidity)
    (csr/delta-put! [:acme :core :fx] :carry [0.07 0.62 0.0])
    (csr/delta-put! [:initech :special :synthetic] :risk [0.66 0.55 0.0])))

(defn merged-row-capacity
  [source delta row-key]
  (+ (let [row-id (csr/csr-row-id source row-key)]
       (if (neg? row-id)
         0
         (let [[start end] (csr/csr-row-span source row-id)]
           (- end start))))
     (count (csr/delta-row-entries delta row-key))))

(defn materialize-merged-row
  [source delta row-key]
  (let [entry-capacity (merged-row-capacity source delta row-key)
        block-dim (csr/csr-block-dim source)
        rows (int-array entry-capacity)
        cols (int-array entry-capacity)
        entries (int-array entry-capacity)
        row-keys (object-array entry-capacity)
        col-keys (object-array entry-capacity)
        origins (object-array entry-capacity)
        blocks (double-array (* entry-capacity block-dim))
        cursor (int-array 1)]
    (csr/scan-merged-row!
     source
     delta
     row-key
     (fn [row-id row-key* col-id col-key origin entry-id block]
       (let [i (aget cursor 0)
             block-off (* i block-dim)]
         (aset-int rows i row-id)
         (aset-int cols i col-id)
         (aset-int entries i entry-id)
         (aset row-keys i row-key*)
         (aset col-keys i col-key)
         (aset origins i origin)
         (if (= :main origin)
           (csr/csr-copy-block! source entry-id blocks block-off)
           (System/arraycopy ^doubles block 0 blocks block-off block-dim))
         (aset-int cursor 0 (inc i)))))
    {:source source
     :rows rows
     :row-keys row-keys
     :cols cols
     :col-keys col-keys
     :entries entries
     :origins origins
     :blocks blocks
     :count (aget cursor 0)}))

(clerk/table
 (materialized-scan->table
  (materialize-merged-row source delta [:acme :core :fx])))

;; Delta-only rows are supported too. They use row id `-1` because they are not
;; yet present in the immutable main CSR.

(clerk/table
 (materialized-scan->table
  (materialize-merged-row source delta [:initech :special :synthetic])))

;; ## 7. Persist and reopen as a language-neutral mmap artifact
;;
;; The artifact writer accepts any `CSRSource`, not just generated datasets. The
;; file is a single little-endian binary artifact with a header, section table,
;; primitive CSR sections, payload doubles, and typed row/column key
;; dictionaries.

(def artifact-path
  (Paths/get "/tmp/sparse-layout-demo.slcsr" (make-array String 0)))

(csr/write-csr-artifact! source artifact-path)

(def mmap-source
  (csr/open-csr-artifact artifact-path))

(clerk/table
 [(select-keys (csr/csr-artifact-metadata mmap-source)
               [:row-count :col-count :entry-count :block-dim])])

(clerk/table
 (materialized-scan->table
  (materialize-source-row mmap-source 0)))

;; ## 8. What mmap needs to preserve
;;
;; Other languages do not need Clojure internals to read the core layout. They
;; need only the binary header, the section table, primitive sections, and the
;; tagged key dictionary encoding.

(clerk/table
 (mapv (fn [[section fields]]
         {:artifact-section section
          :fields fields})
       csr/mmap-artifact-format-sketch))

;; ## 9. Takeaway
;;
;; `sparse-layout` is moving toward a physical layout compiler:
;;
;; - `defsparse` keeps the logical API concise.
;; - Freeze compiles nested data into dictionary-encoded primitive arrays.
;; - `CSRSource` decouples hot readers from generated dataset types.
;; - Range indexes encode query shape without changing the public dataset API.
;; - Mmap artifacts make the physical layout durable and language-neutral.
;; - Deltas make mutable overlays possible while preserving immutable main
;;   storage.

(clerk/html
 [:div {:style {:display "grid"
                :grid-template-columns "repeat(3, minmax(0, 1fr))"
                :gap ".75rem"}}
  (for [[label value] [["Rows" (csr/csr-row-count source)]
                       ["Entries" (csr/csr-entry-count source)]
                       ["Block dim" (csr/csr-block-dim source)]]]
    [:div {:style {:padding "1rem"
                   :border-radius "8px"}}
     [:div {:style {:font-size ".8rem"
                    :text-transform "uppercase"}}
      label]
     [:div {:style {:font-size "1.8rem"
                    :font-weight "700"}}
      value]])])
