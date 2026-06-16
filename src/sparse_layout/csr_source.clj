(ns sparse-layout.csr-source
  (:require [sparse-layout.core :as sparse])
  (:import [java.util HashMap Map]))

(set! *warn-on-reflection* true)

(def ^:private double-array-class
  (Class/forName "[D"))

(def mmap-artifact-format-sketch
  "Version-0 sketch for future mmap-backed CSR artifacts. This is descriptive
  metadata for the backend boundary; no reader/writer is implemented yet."
  {:header [:magic :version :endian-marker :row-count :entry-count :block-dim :section-table-offset]
   :section-table [:name :primitive-type :count :byte-offset :byte-length]
   :sections [:row-ptrs
              :col-ids
              :payload-values
              :row-key-columns
              :metadata]})

(defprotocol CSRSource
  (csr-row-count [src])
  (csr-entry-count [src])
  (csr-block-dim [src])
  (csr-row-id [src row-key])
  (csr-col-id [src col-key])
  (csr-row-key-at [src row-id])
  (csr-col-key-at [src col-id])
  (csr-row-span [src row-id])
  (csr-entry-col-id [src entry-id])
  (csr-copy-block! [src entry-id dst dst-off])
  (csr-scan-row! [src row-id visitor])
  (csr-resolve-ranges [src selection])
  (csr-scan-ranges! [src ranges visitor]))

(defprotocol MutableSparseDelta
  (delta-put! [delta row-key col-key block])
  (delta-delete! [delta row-key col-key])
  (delta-row-entries [delta row-key])
  (delta-entry [delta row-key col-key])
  (delta-clear! [delta]))

(defprotocol MergedCSRSource
  (scan-merged-row! [main delta row-key visitor])
  (scan-merged-ranges! [main delta ranges visitor])
  (compact-to-sink! [main delta sink]))

(defn- fixed-double-block? [internals]
  (and (= :fixed-double-block (:payload-kind internals))
       (pos? (long (:payload-dim internals)))))

(defn- require-fixed-double-block! [internals]
  (when-not (fixed-double-block? internals)
    (throw (ex-info "CSRSource currently supports fixed double block payloads only."
                    {:payload-kind (:payload-kind internals)
                     :payload-dim (:payload-dim internals)}))))

(defn- require-csr! [internals]
  (when (or (nil? (:csr-row-ptrs internals))
            (nil? (:csr-col-ids internals)))
    (throw (ex-info "CSRSource requires a compiled CSR index."
                    {:required :csr}))))

(defn- row-range
  [start end]
  {:row-start (int start)
   :row-end (int end)})

(defn- normalize-range [r]
  (cond
    (map? r) [(int (:row-start r)) (int (:row-end r))]
    (vector? r) [(int (nth r 0)) (int (nth r 1))]
    (seq? r) [(int (first r)) (int (second r))]
    :else (throw (ex-info "Unsupported CSR row range."
                          {:range r}))))

(defn- normalize-prefix [prefix]
  (cond
    (nil? prefix) []
    (vector? prefix) prefix
    (sequential? prefix) (vec prefix)
    :else [prefix]))

(defn- prefixes [value]
  (let [v (normalize-prefix value)
        n (count v)]
    (mapv #(subvec v 0 %) (range 0 (inc n)))))

(defn- rows->ranges [rows]
  (let [rows (->> rows (sort) (map int) vec)
        n (count rows)]
    (loop [idx 0
           start nil
           prev nil
           out (transient [])]
      (if (= idx n)
        (persistent!
         (if (nil? start)
           out
           (conj! out (row-range start (inc prev)))))
        (let [row (nth rows idx)]
          (if (nil? start)
            (recur (inc idx) row row out)
            (if (= row (inc prev))
              (recur (inc idx) start row out)
              (recur (inc idx)
                     row
                     row
                     (conj! out (row-range start (inc prev)))))))))))

(defn- build-range-index [source prefix-fn]
  (let [acc (HashMap.)
        row-count (csr-row-count source)]
    (dotimes [row-id row-count]
      (let [row-key (csr-row-key-at source row-id)]
        (doseq [prefix (prefixes (prefix-fn row-key))]
          (let [rows (or (.get acc prefix) [])]
            (.put acc prefix (conj rows row-id))))))
    (into {} (map (fn [[prefix rows]]
                    [prefix (rows->ranges rows)])
                  acc))))

(declare with-range-index)

(deftype HeapCSRSource
  [^Map rowToId
   ^objects idToRow
   ^Map colToId
   ^objects idToCol
   ^ints rowPtrs
   ^ints colIds
   ^doubles payloadValues
   ^long payloadDim
   rangeIndex
   prefixFn]
  CSRSource
  (csr-row-count [_]
    (alength idToRow))
  (csr-entry-count [_]
    (alength colIds))
  (csr-block-dim [_]
    payloadDim)
  (csr-row-id [_ row-key]
    (sparse/id-of rowToId row-key))
  (csr-col-id [_ col-key]
    (sparse/id-of colToId col-key))
  (csr-row-key-at [_ row-id]
    (sparse/key-of idToRow row-id))
  (csr-col-key-at [_ col-id]
    (sparse/key-of idToCol col-id))
  (csr-row-span [_ row-id]
    [(aget rowPtrs (int row-id))
     (aget rowPtrs (inc (int row-id)))])
  (csr-entry-col-id [_ entry-id]
    (aget colIds (int entry-id)))
  (csr-copy-block! [_ entry-id dst dst-off]
    (let [dst ^doubles dst
          start (* (long entry-id) payloadDim)]
      (System/arraycopy payloadValues (int start) dst (int dst-off) (int payloadDim))
      dst))
  (csr-scan-row! [this row-id visitor]
    (let [row-id* (int row-id)
          start (aget rowPtrs row-id*)
          end (aget rowPtrs (inc row-id*))]
      (loop [entry-id start]
        (when (< entry-id end)
          (visitor row-id* (aget colIds entry-id) entry-id this)
          (recur (inc entry-id)))))
    nil)
  (csr-resolve-ranges [_ selection]
    (cond
      (nil? selection) [(row-range 0 (alength idToRow))]
      (:ranges selection) (:ranges selection)
      (:range selection) [(:range selection)]
      :else
      (let [prefix (normalize-prefix (:prefix selection))]
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
          (when (< row-id end)
            (csr-scan-row! this row-id visitor)
            (recur (inc row-id))))))
    nil))

(deftype MmapCSRSource
  [path
   mapped
   sectionTable
   rowPtrsSection
   colIdsSection
   payloadSection
   ^long payloadDim
   rangeIndex]
  CSRSource
  (csr-row-count [_]
    (throw (ex-info "MmapCSRSource is a storage-backend skeleton; row count is not implemented yet."
                    {:path path})))
  (csr-entry-count [_]
    (throw (ex-info "MmapCSRSource is a storage-backend skeleton; entry count is not implemented yet."
                    {:path path})))
  (csr-block-dim [_]
    payloadDim)
  (csr-row-id [_ row-key]
    (throw (ex-info "MmapCSRSource row lookup is not implemented yet."
                    {:path path :row-key row-key})))
  (csr-col-id [_ col-key]
    (throw (ex-info "MmapCSRSource column lookup is not implemented yet."
                    {:path path :col-key col-key})))
  (csr-row-key-at [_ row-id]
    (throw (ex-info "MmapCSRSource row-key access is not implemented yet."
                    {:path path :row-id row-id})))
  (csr-col-key-at [_ col-id]
    (throw (ex-info "MmapCSRSource column-key access is not implemented yet."
                    {:path path :col-id col-id})))
  (csr-row-span [_ row-id]
    (throw (ex-info "MmapCSRSource row span access is not implemented yet."
                    {:path path :row-id row-id})))
  (csr-entry-col-id [_ entry-id]
    (throw (ex-info "MmapCSRSource entry column access is not implemented yet."
                    {:path path :entry-id entry-id})))
  (csr-copy-block! [_ entry-id _ _]
    (throw (ex-info "MmapCSRSource block copy is not implemented yet."
                    {:path path :entry-id entry-id})))
  (csr-scan-row! [_ row-id _]
    (throw (ex-info "MmapCSRSource row scan is not implemented yet."
                    {:path path :row-id row-id})))
  (csr-resolve-ranges [_ selection]
    (if rangeIndex
      (get rangeIndex (normalize-prefix (:prefix selection)) [])
      (throw (ex-info "MmapCSRSource range resolution is not implemented yet."
                      {:path path :selection selection}))))
  (csr-scan-ranges! [_ ranges _]
    (throw (ex-info "MmapCSRSource range scan is not implemented yet."
                    {:path path :ranges ranges}))))

(defn- validate-mmap-skeleton!
  [{:keys [path payload-dim section-table row-ptrs-section col-ids-section payload-section]}]
  (when-not path
    (throw (ex-info "MmapCSRSource skeleton requires a path."
                    {:required :path})))
  (when-not (pos-int? payload-dim)
    (throw (ex-info "MmapCSRSource skeleton requires a positive fixed block dimension."
                    {:path path
                     :payload-dim payload-dim})))
  (when (and section-table (not (map? section-table)))
    (throw (ex-info "MmapCSRSource section table must be a map when provided."
                    {:path path
                     :section-table-class (class section-table)})))
  (doseq [[section-name section] [[:row-ptrs row-ptrs-section]
                                  [:col-ids col-ids-section]
                                  [:payload-values payload-section]]]
    (when (and section (not (map? section)))
      (throw (ex-info "MmapCSRSource section descriptors must be maps when provided."
                      {:path path
                       :section section-name
                       :section-class (class section)})))))

(defn dataset->csr-source
  "Adapts a frozen sparse-layout dataset to a heap-backed fixed-double-block
  CSRSource. This is a storage-level API: scans expose entry ids and copy into
  caller-owned buffers instead of allocating one block per entry."
  [dataset]
  (let [internals (sparse/sparse-internals dataset)]
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
    (cond
      (instance? HeapCSRSource source)
      (->HeapCSRSource (.-rowToId ^HeapCSRSource source)
                       (.-idToRow ^HeapCSRSource source)
                       (.-colToId ^HeapCSRSource source)
                       (.-idToCol ^HeapCSRSource source)
                       (.-rowPtrs ^HeapCSRSource source)
                       (.-colIds ^HeapCSRSource source)
                       (.-payloadValues ^HeapCSRSource source)
                       (.-payloadDim ^HeapCSRSource source)
                       range-index
                       prefix-fn)

      (instance? MmapCSRSource source)
      (->MmapCSRSource (.-path ^MmapCSRSource source)
                       (.-mapped ^MmapCSRSource source)
                       (.-sectionTable ^MmapCSRSource source)
                       (.-rowPtrsSection ^MmapCSRSource source)
                       (.-colIdsSection ^MmapCSRSource source)
                       (.-payloadSection ^MmapCSRSource source)
                       (.-payloadDim ^MmapCSRSource source)
                       range-index)

      :else
      (throw (ex-info "Unsupported CSRSource implementation for range indexing."
                      {:class (class source)})))))

(defn make-mmap-csr-source-skeleton
  "Creates a placeholder mmap CSR source. The type exists so consumers can
  depend on CSRSource instead of heap arrays while the artifact reader is built.
  See `mmap-artifact-format-sketch` for the intended artifact sections."
  [{:keys [path mapped section-table row-ptrs-section col-ids-section payload-section payload-dim range-index]
    :as opts}]
  (validate-mmap-skeleton! opts)
  (->MmapCSRSource path
                   mapped
                   section-table
                   row-ptrs-section
                   col-ids-section
                   payload-section
                   (long payload-dim)
                   range-index))

(defn- copy-block [block]
  (cond
    (instance? double-array-class block)
    (let [src ^doubles block
          out (double-array (alength src))]
      (System/arraycopy src 0 out 0 (alength src))
      out)

    (sequential? block)
    (double-array (map double block))

    :else
    (throw (ex-info "Delta fixed-double-block values must be double arrays or numeric sequences."
                    {:value block
                     :class (some-> block class str)}))))

(deftype DokDelta
  [^HashMap rows]
  MutableSparseDelta
  (delta-put! [this row-key col-key block]
    (let [^HashMap row (or (.get rows row-key)
                           (let [m (HashMap.)]
                             (.put rows row-key m)
                             m))]
      (.put row col-key {:op :put
                         :row-key row-key
                         :col-key col-key
                         :block (copy-block block)})
      this))
  (delta-delete! [this row-key col-key]
    (let [^HashMap row (or (.get rows row-key)
                           (let [m (HashMap.)]
                             (.put rows row-key m)
                             m))]
      (.put row col-key {:op :delete
                         :row-key row-key
                         :col-key col-key})
      this))
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
  (delta-clear! [this]
    (.clear rows)
    this))

(defn make-dok-delta []
  (->DokDelta (HashMap.)))

(defn- col-sort-key [source col-key]
  (let [col-id (csr-col-id source col-key)]
    (if (neg? col-id)
      [1 (pr-str col-key)]
      [0 col-id])))

(defn- sorted-delta-row [source delta row-key]
  (->> (delta-row-entries delta row-key)
       (sort-by (fn [{:keys [col-key]}]
                  (col-sort-key source col-key)))
       vec))

(defn- emit-main!
  [source row-id row-key entry-id visitor]
  (let [col-id (csr-entry-col-id source entry-id)
        col-key (csr-col-key-at source col-id)]
    (visitor row-id row-key col-id col-key :main entry-id nil)))

(defn- emit-delta!
  [source row-id row-key {:keys [col-key block] :as entry} visitor]
  (let [col-id (csr-col-id source col-key)]
    (visitor row-id row-key col-id col-key :delta -1 block)
    entry))

(defn- scan-delta-only-row! [source delta row-key visitor]
  (doseq [{:keys [op] :as entry} (sorted-delta-row source delta row-key)]
    (when (= :put op)
      (emit-delta! source -1 row-key entry visitor)))
  nil)

(defn scan-merged-row-by-id!
  "Scans a main row id with delta entries merged over it. Visitor arity is:
  `(visitor row-id row-key col-id col-key origin entry-id block)`.
  Main entries pass `origin` `:main`, the main `entry-id`, and nil `block`;
  delta entries pass `origin` `:delta`, entry id -1, and the delta-owned block."
  [source delta row-id visitor]
  (let [row-key (csr-row-key-at source row-id)
        [start end] (csr-row-span source row-id)
        delta-entries (sorted-delta-row source delta row-key)
        delta-count (count delta-entries)]
    (loop [main-entry start
           delta-idx 0]
      (cond
        (and (= main-entry end) (= delta-idx delta-count))
        nil

        (= main-entry end)
        (let [{:keys [op] :as delta-entry} (nth delta-entries delta-idx)]
          (when (= :put op)
            (emit-delta! source row-id row-key delta-entry visitor))
          (recur main-entry (inc delta-idx)))

        (= delta-idx delta-count)
        (do
          (emit-main! source row-id row-key main-entry visitor)
          (recur (inc main-entry) delta-idx))

        :else
        (let [main-col-id (csr-entry-col-id source main-entry)
              main-col-key (csr-col-key-at source main-col-id)
              delta-entry (nth delta-entries delta-idx)
              delta-col-key (:col-key delta-entry)
              delta-col-id (csr-col-id source delta-col-key)
              delta-key (col-sort-key source delta-col-key)
              main-key [0 main-col-id]]
          (cond
            (= main-col-key delta-col-key)
            (do
              (when (= :put (:op delta-entry))
                (emit-delta! source row-id row-key delta-entry visitor))
              (recur (inc main-entry) (inc delta-idx)))

            (or (neg? delta-col-id)
                (pos? (compare delta-key main-key)))
            (do
              (emit-main! source row-id row-key main-entry visitor)
              (recur (inc main-entry) delta-idx))

            :else
            (do
              (when (= :put (:op delta-entry))
                (emit-delta! source row-id row-key delta-entry visitor))
              (recur main-entry (inc delta-idx)))))))))

(extend-type HeapCSRSource
  MergedCSRSource
  (scan-merged-row! [main delta row-key visitor]
    (let [row-id (csr-row-id main row-key)]
      (if (neg? row-id)
        (scan-delta-only-row! main delta row-key visitor)
        (scan-merged-row-by-id! main delta row-id visitor)))
    nil)
  (scan-merged-ranges! [main delta ranges visitor]
    (doseq [r ranges]
      (let [[start end] (normalize-range r)]
        (loop [row-id start]
          (when (< row-id end)
            (scan-merged-row-by-id! main delta row-id visitor)
            (recur (inc row-id))))))
    nil)
  (compact-to-sink! [_ _ _]
    (throw (ex-info "Compaction sinks are not implemented yet."
                    {:source :heap-csr}))))
