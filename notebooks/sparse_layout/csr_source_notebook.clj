(ns sparse-layout.csr-source-notebook
  (:require [nextjournal.clerk :as clerk]
            [sparse-layout.core :refer [block-view-value
                                        sparse-internals]]
            [sparse-layout.notebook-data :as demo]
            [sparse-layout.csr-source :as csr]))

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
                :border "1px solid #d8dee9"
                :border-radius "8px"
                :background "#f8fafc"}}
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

(clerk/table demo/records)

;; ## 2. The generated API stays ergonomic
;;
;; Existing callers can keep using the generated row, column, point, and view
;; APIs. The new source layer is additive.

(demo/portfolio-features-row demo/dataset [:acme :core :fx])

(def exposure-view
  (demo/portfolio-features-block-view demo/dataset [:acme :core :fx] :exposure))

[(block-view-value exposure-view 0)
 (block-view-value exposure-view 1)
 (block-view-value exposure-view 2)]

;; ## 3. The frozen shape is compact CSR plus dictionaries
;;
;; The generated dataset exposes internals for adapters. The important CSR
;; fields are row pointers, column ids, and one contiguous fixed-double-block
;; payload array.

(def internals
  (sparse-internals demo/dataset))

(def physical-summary
  {:rows (alength ^objects (:id->row internals))
   :cols (alength ^objects (:id->col internals))
   :entries (alength ^ints (:csr-col-ids internals))
   :payload-kind (:payload-kind internals)
   :block-dim (:payload-dim internals)
   :payload-doubles (alength ^doubles (:payload-values internals))})

(clerk/table [physical-summary])

;; ## 4. Adapt the frozen dataset to CSRSource
;;
;; `CSRSource` is the storage boundary. Consumers ask for spans, column ids, and
;; block copies; they do not need to know whether the backing store is a heap
;; array today or an mmap artifact later.

(def source
  (csr/dataset->csr-source demo/dataset))

(defn copy-block
  [source entry-id]
  (let [out (double-array (csr/csr-block-dim source))]
    (csr/csr-copy-block! source entry-id out 0)
    (vec (seq out))))

(defn source-row
  [source row-id]
  (let [row-key (csr/csr-row-key-at source row-id)
        seen (atom [])]
    (csr/csr-scan-row!
     source
     row-id
     (fn [row-id* col-id entry-id src]
       (swap! seen conj
              {:row-id row-id*
               :row-key row-key
               :entry-id entry-id
               :col-id col-id
               :col-key (csr/csr-col-key-at src col-id)
               :block (copy-block src entry-id)})))
    @seen))

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

;; Scan one row through the storage-level API. Notice that payloads are copied
;; into a caller-owned `double[]` by `csr-copy-block!`.

(clerk/table (source-row source 0))

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

(defn scan-ranges
  [source ranges]
  (let [seen (atom [])]
    (csr/csr-scan-ranges!
     source
     ranges
     (fn [row-id col-id entry-id src]
       (swap! seen conj
              {:row-key (csr/csr-row-key-at src row-id)
               :col-key (csr/csr-col-key-at src col-id)
               :entry-id entry-id
               :block (copy-block src entry-id)})))
    @seen))

(clerk/table (scan-ranges ranged-source acme-core-ranges))

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

(defn merged-row
  [source delta row-key]
  (let [seen (atom [])]
    (csr/scan-merged-row!
     source
     delta
     row-key
     (fn [row-id row-key* col-id col-key origin entry-id block]
       (swap! seen conj
              {:row-id row-id
               :row-key row-key*
               :col-id col-id
               :col-key col-key
               :origin origin
               :entry-id entry-id
               :block (if (= :main origin)
                        (copy-block source entry-id)
                        (vec (seq block)))})))
    @seen))

(clerk/table (merged-row source delta [:acme :core :fx]))

;; Delta-only rows are supported too. They use row id `-1` because they are not
;; yet present in the immutable main CSR.

(clerk/table (merged-row source delta [:initech :special :synthetic]))

;; ## 7. What mmap needs to preserve
;;
;; The future mmap backend can use a different physical store as long as it
;; satisfies the same CSRSource operations. The current code captures the
;; intended artifact sections as data.

(clerk/table
 (mapv (fn [[section fields]]
         {:artifact-section section
          :fields fields})
       csr/mmap-artifact-format-sketch))

;; ## 8. Takeaway
;;
;; `sparse-layout` is moving toward a physical layout compiler:
;;
;; - `defsparse` keeps the logical API concise.
;; - Freeze compiles nested data into dictionary-encoded primitive arrays.
;; - `CSRSource` decouples hot readers from generated dataset types.
;; - Range indexes encode query shape without changing the public dataset API.
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
                   :border "1px solid #d8dee9"
                   :border-radius "8px"
                   :background "#ffffff"}}
     [:div {:style {:font-size ".8rem"
                    :color "#64748b"
                    :text-transform "uppercase"}}
      label]
     [:div {:style {:font-size "1.8rem"
                    :font-weight "700"}}
      value]])])
