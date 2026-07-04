(ns sparse-layout.csr-source
  (:require [sparse-layout.core :as sparse]
            [sparse-layout.csr-source.artifact :as artifact]
            [sparse-layout.csr-source.protocols :as p :refer
             [CSRSource MutableSparseDelta MergedCSRSource]])
  (:import [sparse_layout.csr_source.artifact MmapCSRSource]
           [java.util HashMap Map]))

(def ^:private double-array-class (Class/forName "[D"))

(defn- fixed-double-block?
  [internals]
  (and (= :fixed-double-block (:payload-kind internals)) (pos? (long (:payload-dim internals)))))

(defn- require-fixed-double-block!
  [internals]
  (when-not (fixed-double-block? internals)
    (throw (ex-info "CSRSource currently supports fixed double block payloads only."
                    {:payload-kind (:payload-kind internals)
                     :payload-dim (:payload-dim internals)}))))

(defn- require-csr!
  [internals]
  (when (or (nil? (:csr-row-ptrs internals)) (nil? (:csr-col-ids internals)))
    (throw (ex-info "CSRSource requires a compiled CSR index." {:required :csr}))))

(defn- no-sparse-internals-impl?
  [^IllegalArgumentException e]
  (boolean (some-> (.getMessage e)
                   (.contains "No implementation of method: :sparse-internals"))))

(defn- public-field-value
  [dataset field-name cause]
  (try (let [field (.getField (class dataset) field-name)]
         (.get field dataset))
       (catch NoSuchFieldException e
         (throw (ex-info
                  "Dataset does not implement SparseInternals and does not expose defsparse fields"
                  {:dataset-class (class dataset) :missing-field field-name}
                  (or cause e))))))

(defn- generated-field-internals
  [dataset cause]
  {:row->id (public-field-value dataset "rowToId" cause)
   :id->row (public-field-value dataset "idToRow" cause)
   :col->id (public-field-value dataset "colToId" cause)
   :id->col (public-field-value dataset "idToCol" cause)
   :edge-rows (public-field-value dataset "edgeRows" cause)
   :edge-cols (public-field-value dataset "edgeCols" cause)
   :csr-row-ptrs (public-field-value dataset "csrRowPtrs" cause)
   :csr-col-ids (public-field-value dataset "csrColIds" cause)
   :csc-col-ptrs (public-field-value dataset "cscColPtrs" cause)
   :csc-row-ids (public-field-value dataset "cscRowIds" cause)
   :csc-payload-ids (public-field-value dataset "cscPayloadIds" cause)
   :payload-kind (public-field-value dataset "payloadKind" cause)
   :payload-dim (public-field-value dataset "payloadDim" cause)
   :payload-values (public-field-value dataset "payloadValues" cause)
   :payload-ptrs (public-field-value dataset "payloadPtrs" cause)})

(defn- sparse-internals*
  [dataset]
  (try (sparse/sparse-internals dataset)
       (catch IllegalArgumentException e
         (if (no-sparse-internals-impl? e) (generated-field-internals dataset e) (throw e)))))

(defn- row-range [start end] {:row-start (int start) :row-end (int end)})

(defn- normalize-range
  [r]
  (cond (map? r) [(int (:row-start r)) (int (:row-end r))]
        (vector? r) [(int (nth r 0)) (int (nth r 1))]
        (seq? r) [(int (first r)) (int (second r))]
        :else (throw (ex-info "Unsupported CSR row range." {:range r}))))

(defn- normalize-prefix
  [prefix]
  (cond (nil? prefix) []
        (vector? prefix) prefix
        (sequential? prefix) (vec prefix)
        :else [prefix]))

(defn- prefixes
  [value]
  (let [v
        (normalize-prefix value)

        n
        (count v)]

    (mapv #(subvec v 0 %) (range 0 (inc n)))))

(defn- rows->ranges
  [rows]
  (let [rows
        (->> rows
             (sort)
             (map int)
             vec)

        n
        (count rows)]

    (loop [idx
           0

           start
           nil

           prev
           nil

           out
           (transient [])]

      (if (= idx n)
        (persistent! (if (nil? start) out (conj! out (row-range start (inc prev)))))
        (let [row (nth rows idx)]
          (if (nil? start)
            (recur (inc idx) row row out)
            (if (= row (inc prev))
              (recur (inc idx) start row out)
              (recur (inc idx) row row (conj! out (row-range start (inc prev)))))))))))

(defn- build-range-index
  [source prefix-fn]
  (let [acc
        (HashMap.)

        row-count
        (p/csr-row-count source)]

    (dotimes [row-id row-count]
      (let [row-key (p/csr-row-key-at source row-id)]
        (doseq [prefix (prefixes (prefix-fn row-key))]
          (let [rows (or (.get acc prefix) [])]
            (.put acc prefix (conj rows row-id))))))
    (into {}
          (map (fn [[prefix rows]]
                 [prefix (rows->ranges rows)])
               acc))))

(defn- ensure-index!
  [label value upper-bound]
  (let [value
        (long value)

        upper-bound
        (long upper-bound)]

    (when (or (neg? value) (>= value upper-bound))
      (throw (ex-info "CSRSource index is out of bounds."
                      {:index label :value value :upper-bound upper-bound})))
    value))

(defn- ensure-copy-target!
  [dst dst-off block-dim]
  (let [dst-len
        (alength ^doubles dst)

        dst-off
        (long dst-off)

        block-dim
        (long block-dim)]

    (when (or (neg? dst-off) (> (+ dst-off block-dim) dst-len))
      (throw (ex-info "CSRSource copy destination is out of bounds."
                      {:dst-off dst-off :block-dim block-dim :dst-length dst-len})))
    dst-off))

(deftype HeapCSRSource [^Map rowToId ^objects idToRow ^Map colToId ^objects idToCol ^ints rowPtrs
                        ^ints colIds ^doubles payloadValues ^long payloadDim rangeIndex prefixFn]
  CSRSource
    (csr-row-count [_] (alength idToRow))
    (csr-col-count [_] (alength idToCol))
    (csr-entry-count [_] (alength colIds))
    (csr-block-dim [_] payloadDim)
    (csr-row-id [_ row-key] (sparse/id-of rowToId row-key))
    (csr-col-id [_ col-key] (sparse/id-of colToId col-key))
    (csr-row-key-at [_ row-id]
      (sparse/key-of idToRow (ensure-index! :row-id row-id (alength idToRow))))
    (csr-col-key-at [_ col-id]
      (sparse/key-of idToCol (ensure-index! :col-id col-id (alength idToCol))))
    (csr-row-span [_ row-id]
      (let [row-id (ensure-index! :row-id row-id (alength idToRow))]
        [(aget rowPtrs (int row-id)) (aget rowPtrs (inc (int row-id)))]))
    (csr-entry-col-id [_ entry-id]
      (aget colIds (int (ensure-index! :entry-id entry-id (alength colIds)))))
    (csr-copy-block! [_ entry-id dst dst-off]
      (let [dst
            ^doubles dst

            entry-id
            (ensure-index! :entry-id entry-id (alength colIds))

            dst-off
            (ensure-copy-target! dst dst-off payloadDim)

            start
            (* entry-id payloadDim)]

        (System/arraycopy payloadValues (int start) dst (int dst-off) (int payloadDim))
        dst))
    (csr-scan-row! [this row-id visitor]
      (let [row-id*
            (int (ensure-index! :row-id row-id (alength idToRow)))

            start
            (aget rowPtrs row-id*)

            end
            (aget rowPtrs (inc row-id*))]

        (loop [entry-id start]
          (when (< entry-id end)
            (visitor row-id* (aget colIds entry-id) entry-id this)
            (recur (inc entry-id)))))
      nil)
    (csr-resolve-ranges [_ selection]
      (cond (nil? selection) [(row-range 0 (alength idToRow))]
            (:ranges selection) (:ranges selection)
            (:range selection) [(:range selection)]
            :else (let [prefix (normalize-prefix (:prefix selection))]
                    (if (empty? prefix)
                      [(row-range 0 (alength idToRow))]
                      (if rangeIndex
                        (get rangeIndex prefix [])
                        (throw (ex-info "CSRSource has no range index."
                                        {:selection selection})))))))
    (csr-scan-ranges! [this ranges visitor]
      (doseq [r ranges]
        (let [[start end] (normalize-range r)]
          (loop [row-id start]
            (when (< row-id end) (p/csr-scan-row! this row-id visitor) (recur (inc row-id))))))
      nil))

(def write-csr-artifact! artifact/write-csr-artifact!)
(def open-csr-artifact artifact/open-csr-artifact)
(def csr-artifact-metadata artifact/csr-artifact-metadata)

(defn dataset->csr-source
  "Adapts a frozen sparse-layout dataset to a heap-backed fixed-double-block
  CSRSource. This is a storage-level API: scans expose entry ids and copy into
  caller-owned buffers instead of allocating one block per entry."
  [dataset]
  (let [internals (sparse-internals* dataset)]
    (require-csr! internals)
    (require-fixed-double-block! internals)
    (->HeapCSRSource (:row->id internals)
                     (:id->row internals)
                     (:col->id internals)
                     (:id->col internals)
                     (:csr-row-ptrs internals)
                     (:csr-col-ids internals)
                     (:payload-values internals)
                     (long (:payload-dim internals))
                     nil
                     nil)))

(defn with-range-index
  "Returns `source` with a prefix range index built from `(prefix-fn row-key)`.
  Every leading prefix is indexed, so a row key projected to `[a b c]` is
  reachable by `[]`, `[a]`, `[a b]`, and `[a b c]`. Non-contiguous matches are
  represented as multiple ranges."
  [source prefix-fn]
  (let [range-index (build-range-index source prefix-fn)]
    (cond (instance? HeapCSRSource source) (->HeapCSRSource (.-rowToId ^HeapCSRSource source)
                                                            (.-idToRow ^HeapCSRSource source)
                                                            (.-colToId ^HeapCSRSource source)
                                                            (.-idToCol ^HeapCSRSource source)
                                                            (.-rowPtrs ^HeapCSRSource source)
                                                            (.-colIds ^HeapCSRSource source)
                                                            (.-payloadValues ^HeapCSRSource source)
                                                            (.-payloadDim ^HeapCSRSource source)
                                                            range-index
                                                            prefix-fn)
          (artifact/mmap-csr-source? source)
          (artifact/with-mmap-range-index source prefix-fn range-index)
          :else (throw (ex-info "Unsupported CSRSource implementation for range indexing."
                                {:class (class source)})))))

(defn- normalize-block-dim
  [block-dim]
  (if (and (integer? block-dim) (pos? (long block-dim)))
    (long block-dim)
    (throw (ex-info "Delta block dimension must be a positive integer." {:block-dim block-dim}))))

(defn- ensure-block-length!
  [block expected]
  (let [actual (alength ^doubles block)]
    (when-not (= (long expected) actual)
      (throw (ex-info "Delta fixed-double-block value has the wrong dimension."
                      {:expected (long expected) :actual actual}))))
  block)

(defn- copy-block
  [block expected]
  (let [out
        (cond (instance? double-array-class block) (let [src ^doubles block
                                                         out (double-array (alength src))]

                                                     (System/arraycopy src 0 out 0 (alength src))
                                                     out)
              (sequential? block) (double-array (map double block))
              :else
              (throw (ex-info
                       "Delta fixed-double-block values must be double arrays or numeric sequences."
                       {:value block
                        :class (some-> block
                                       class
                                       str)})))]
    (ensure-block-length! out expected)))

(deftype DokDelta [^HashMap rows blockDim ^:unsynchronized-mutable ^long version]
  MutableSparseDelta
    (delta-put! [this row-key col-key block]
      (let [^HashMap row (or (.get rows row-key)
                             (let [m (HashMap.)]
                               (.put rows row-key m)
                               m))]
        (.put row
              col-key
              {:op :put :row-key row-key :col-key col-key :block (copy-block block blockDim)})
        (set! version (inc version))
        this))
    (delta-delete! [this row-key col-key]
      (let [^HashMap row (or (.get rows row-key)
                             (let [m (HashMap.)]
                               (.put rows row-key m)
                               m))]
        (.put row col-key {:op :delete :row-key row-key :col-key col-key})
        (set! version (inc version))
        this))
    (delta-row-keys [_] (vec (keys rows)))
    (delta-row-entries [_ row-key]
      (if-let [row ^HashMap (.get rows row-key)]
        (->> (vals row)
             (sort-by (fn [{:keys [col-key]}]
                        (pr-str col-key)))
             vec)
        []))
    (delta-entry [_ row-key col-key]
      (when-let [row ^HashMap (.get rows row-key)]
        (.get row col-key)))
    (delta-clear! [this] (.clear rows) (set! version (inc version)) this)
    (delta-version [_] version))

(defn make-dok-delta [block-dim] (->DokDelta (HashMap.) (normalize-block-dim block-dim) 0))

(defn make-dok-delta-for-source [source] (make-dok-delta (p/csr-block-dim source)))

(defn- col-sort-key
  [source col-key]
  (let [col-id (p/csr-col-id source col-key)]
    (if (neg? col-id) [1 (pr-str col-key)] [0 col-id])))

(defn- sorted-delta-row
  [source delta row-key]
  (->> (p/delta-row-entries delta row-key)
       (sort-by (fn [{:keys [col-key]}]
                  (col-sort-key source col-key)))
       vec))

(defn- emit-main!
  [source row-id row-key entry-id visitor]
  (let [col-id
        (p/csr-entry-col-id source entry-id)

        col-key
        (p/csr-col-key-at source col-id)]

    (visitor row-id row-key col-id col-key :main entry-id nil)))

(defn- emit-delta!
  [source row-id row-key {:keys [col-key block] :as entry} visitor]
  (let [col-id (p/csr-col-id source col-key)]
    (ensure-block-length! block (p/csr-block-dim source))
    (visitor row-id row-key col-id col-key :delta -1 block)
    entry))

(defn- scan-delta-only-row!
  [source delta row-key visitor]
  (doseq [{:keys [op] :as entry} (sorted-delta-row source delta row-key)]
    (when (= :put op) (emit-delta! source -1 row-key entry visitor)))
  nil)

(defn scan-merged-row-by-id!
  "Scans a main row id with delta entries merged over it. Visitor arity is:
  `(visitor row-id row-key col-id col-key origin entry-id block)`.
  Main entries pass `origin` `:main`, the main `entry-id`, and nil `block`;
  delta entries pass `origin` `:delta`, entry id -1, and the delta-owned block."
  [source delta row-id visitor]
  (let [row-key
        (p/csr-row-key-at source row-id)

        [start end]
        (p/csr-row-span source row-id)

        delta-entries
        (sorted-delta-row source delta row-key)

        delta-count
        (count delta-entries)]

    (loop [main-entry
           start

           delta-idx
           0]

      (cond (and (= main-entry end) (= delta-idx delta-count)) nil
            (= main-entry end) (let [{:keys [op] :as delta-entry} (nth delta-entries delta-idx)]
                                 (when (= :put op)
                                   (emit-delta! source row-id row-key delta-entry visitor))
                                 (recur main-entry (inc delta-idx)))
            (= delta-idx delta-count) (do (emit-main! source row-id row-key main-entry visitor)
                                          (recur (inc main-entry) delta-idx))
            :else (let [main-col-id
                        (p/csr-entry-col-id source main-entry)

                        main-col-key
                        (p/csr-col-key-at source main-col-id)

                        delta-entry
                        (nth delta-entries delta-idx)

                        delta-col-key
                        (:col-key delta-entry)

                        delta-col-id
                        (p/csr-col-id source delta-col-key)

                        delta-key
                        (col-sort-key source delta-col-key)

                        main-key
                        [0 main-col-id]]

                    (cond (= main-col-key delta-col-key)
                          (do (when (= :put (:op delta-entry))
                                (emit-delta! source row-id row-key delta-entry visitor))
                              (recur (inc main-entry) (inc delta-idx)))
                          (or (neg? delta-col-id) (pos? (compare delta-key main-key)))
                          (do (emit-main! source row-id row-key main-entry visitor)
                              (recur (inc main-entry) delta-idx))
                          :else (do (when (= :put (:op delta-entry))
                                      (emit-delta! source row-id row-key delta-entry visitor))
                                    (recur main-entry (inc delta-idx)))))))))

(defn- scan-merged-row!*
  [main delta row-key visitor]
  (let [row-id (p/csr-row-id main row-key)]
    (if (neg? row-id)
      (scan-delta-only-row! main delta row-key visitor)
      (scan-merged-row-by-id! main delta row-id visitor)))
  nil)

(defn- scan-merged-ranges!*
  [main delta ranges visitor]
  (doseq [r ranges]
    (let [[start end] (normalize-range r)]
      (loop [row-id start]
        (when (< row-id end)
          (scan-merged-row-by-id! main delta row-id visitor)
          (recur (inc row-id))))))
  nil)

(defn- source-prefix-fn
  [source]
  (cond (instance? HeapCSRSource source) (.-prefixFn ^HeapCSRSource source)
        (artifact/mmap-csr-source? source) (artifact/mmap-prefix-fn source)
        :else nil))

(defn- require-prefix-fn!
  [source selection]
  (or (source-prefix-fn source)
      (throw (ex-info "Overlay prefix scans require a CSRSource prepared with with-range-index."
                      {:selection selection :source-class (class source)}))))

(defn- visible-delta-only-row?
  [source delta row-key]
  (and (neg? (p/csr-row-id source row-key))
       (some #(= :put (:op %)) (p/delta-row-entries delta row-key))))

(defn- row-sort-key
  [prefix-fn row-key]
  [(if prefix-fn (pr-str (prefix-fn row-key)) "") (pr-str row-key)])

(defn- sorted-visible-delta-only-row-keys
  [source delta prefix-fn]
  (->> (p/delta-row-keys delta)
       (filter #(visible-delta-only-row? source delta %))
       (sort-by #(row-sort-key prefix-fn %))
       vec))

(defn- build-delta-prefix-index
  [source delta prefix-fn]
  (let [acc (HashMap.)]
    (doseq [row-key (sorted-visible-delta-only-row-keys source delta prefix-fn)]
      (doseq [prefix (prefixes (prefix-fn row-key))]
        (let [rows (or (.get acc prefix) [])]
          (.put acc prefix (conj rows row-key)))))
    (into {} acc)))

(defrecord OverlayCSRView [source delta cache])

(defn overlay-view
  "Returns a logical view of source with delta overlaid.

  Overlay views are not CSRSource implementations: delta-only rows do not have
  stable CSR row ids. Scan visitors receive row-id -1 for delta-only rows."
  [source delta]
  (->OverlayCSRView source delta (volatile! {:version -1 :prefix-index nil})))

(defn- overlay-prefix-index!
  [^OverlayCSRView view prefix-fn]
  (let [source
        (:source view)

        delta
        (:delta view)

        cache
        (:cache view)

        version
        (p/delta-version delta)

        snapshot
        @cache]

    (when (or (nil? (:prefix-index snapshot)) (not= version (:version snapshot)))
      (vreset! cache
               {:version version :prefix-index (build-delta-prefix-index source delta prefix-fn)}))
    (:prefix-index @cache)))

(defn- scan-delta-only-rows!
  [source delta row-keys visitor]
  (doseq [row-key row-keys]
    (when (visible-delta-only-row? source delta row-key)
      (scan-delta-only-row! source delta row-key visitor)))
  nil)

(defn- distinct-preserving-order
  [xs]
  (loop [xs
         (seq xs)

         seen
         #{}

         out
         (transient [])]

    (if-not xs
      (persistent! out)
      (let [x (first xs)]
        (if (contains? seen x)
          (recur (next xs) seen out)
          (recur (next xs) (conj seen x) (conj! out x)))))))

(defn scan-overlay-row!
  "Scans one logical overlay row by row key.

  Visitor arity is `(visitor row-id row-key col-id col-key origin entry-id block)`.
  Delta-only rows use row-id -1. Delta entries use entry-id -1."
  [^OverlayCSRView view row-key visitor]
  (scan-merged-row!* (:source view) (:delta view) row-key visitor))

(defn- scan-overlay-prefix!
  [^OverlayCSRView view prefix selection visitor]
  (let [source
        (:source view)

        delta
        (:delta view)

        prefix
        (normalize-prefix prefix)

        prefix-fn
        (when-not (empty? prefix) (require-prefix-fn! source selection))

        ranges
        (p/csr-resolve-ranges source {:prefix prefix})]

    (scan-merged-ranges!* source delta ranges visitor)
    (if (empty? prefix)
      (if-let [prefix-fn (source-prefix-fn source)]
        (let [index (overlay-prefix-index! view prefix-fn)]
          (scan-delta-only-rows! source delta (get index [] []) visitor))
        (scan-delta-only-rows! source
                               delta
                               (sorted-visible-delta-only-row-keys source delta nil)
                               visitor))
      (let [index (overlay-prefix-index! view prefix-fn)]
        (scan-delta-only-rows! source delta (get index prefix []) visitor))))
  nil)

(defn scan-overlay-selection!
  "Scans a logical overlay selection.

  `nil` and `{:prefix ...}` include visible delta-only rows. Numeric `:range`
  and `:ranges` selections scan only base CSR row-id ranges."
  [^OverlayCSRView view selection visitor]
  (let [source
        (:source view)

        delta
        (:delta view)]

    (cond (nil? selection) (scan-overlay-prefix! view [] selection visitor)
          (contains? selection :row-key) (scan-overlay-row! view (:row-key selection) visitor)
          (contains? selection :row-keys) (doseq [row-key (distinct-preserving-order (:row-keys
                                                                                       selection))]
                                            (scan-overlay-row! view row-key visitor))
          (contains? selection :prefix)
          (scan-overlay-prefix! view (:prefix selection) selection visitor)
          (or (contains? selection :range) (contains? selection :ranges))
          (scan-merged-ranges!* source delta (p/csr-resolve-ranges source selection) visitor)
          :else (throw (ex-info "Unsupported overlay selection." {:selection selection}))))
  nil)

(extend-type HeapCSRSource
  MergedCSRSource
    (scan-merged-row! [main delta row-key visitor] (scan-merged-row!* main delta row-key visitor))
    (scan-merged-ranges! [main delta ranges visitor]
      (scan-merged-ranges!* main delta ranges visitor)))

(extend-type MmapCSRSource
  MergedCSRSource
    (scan-merged-row! [main delta row-key visitor] (scan-merged-row!* main delta row-key visitor))
    (scan-merged-ranges! [main delta ranges visitor]
      (scan-merged-ranges!* main delta ranges visitor)))

(def csr-row-count p/csr-row-count)
(def csr-col-count p/csr-col-count)
(def csr-entry-count p/csr-entry-count)
(def csr-block-dim p/csr-block-dim)
(def csr-row-id p/csr-row-id)
(def csr-col-id p/csr-col-id)
(def csr-row-key-at p/csr-row-key-at)
(def csr-col-key-at p/csr-col-key-at)
(def csr-row-span p/csr-row-span)
(def csr-entry-col-id p/csr-entry-col-id)
(def csr-copy-block! p/csr-copy-block!)
(def csr-scan-row! p/csr-scan-row!)
(def csr-resolve-ranges p/csr-resolve-ranges)
(def csr-scan-ranges! p/csr-scan-ranges!)
(def delta-put! p/delta-put!)
(def delta-delete! p/delta-delete!)
(def delta-row-keys p/delta-row-keys)
(def delta-row-entries p/delta-row-entries)
(def delta-entry p/delta-entry)
(def delta-clear! p/delta-clear!)
(def delta-version p/delta-version)
(def scan-merged-row! p/scan-merged-row!)
(def scan-merged-ranges! p/scan-merged-ranges!)
