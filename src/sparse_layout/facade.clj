(ns sparse-layout.facade
  (:require [sparse-layout.core :as sparse])
  (:import [clojure.lang MapEntry]))

(def ^:private missing
  (Object.))

(defn- unsupported-assoc []
  (throw (UnsupportedOperationException.
          "Sparse facades are immutable read views; rebuild through a sparse builder to add or remove entries.")))

(defn- internals [ds]
  (sparse/sparse-internals ds))

(defn- row-id [internals row-key]
  (sparse/id-of (:row->id internals) row-key))

(defn- col-id [internals col-key]
  (sparse/id-of (:col->id internals) col-key))

(defn- row-key [internals row-id]
  (sparse/key-of (:id->row internals) row-id))

(defn- col-key [internals col-id]
  (sparse/key-of (:id->col internals) col-id))

(defn- payload-id [internals row-id col-id]
  (sparse/payload-id-by-ids row-id
                            col-id
                            (:csr-row-ptrs internals)
                            (:csr-col-ids internals)
                            (:csc-col-ptrs internals)
                            (:csc-row-ids internals)
                            (:csc-payload-ids internals)))

(defn- payload-at [internals mode payload-id]
  (case mode
    :value (sparse/payload-value (:payload-kind internals)
                                 (:payload-dim internals)
                                 (:payload-values internals)
                                 (:payload-ptrs internals)
                                 payload-id)
    :view (sparse/payload-view (:payload-kind internals)
                               (:payload-dim internals)
                               (:payload-values internals)
                               (:payload-ptrs internals)
                               payload-id)))

(defn- row-span [internals row-id]
  (if-let [ptrs ^ints (:csr-row-ptrs internals)]
    [(aget ptrs (int row-id))
     (aget ptrs (inc (int row-id)))]
    (throw (ex-info "CSR index is required for row facade traversal."
                    {:required :csr}))))

(defn- col-span [internals col-id]
  (if-let [ptrs ^ints (:csc-col-ptrs internals)]
    [(aget ptrs (int col-id))
     (aget ptrs (inc (int col-id)))]
    (throw (ex-info "CSC index is required for column facade traversal."
                    {:required :csc}))))

(deftype RowMap [ds internals rowKey ^long rowId mode]
  clojure.lang.ILookup
  (valAt [this col-key]
    (.valAt this col-key nil))
  (valAt [_ col-key not-found]
    (let [col-id (col-id internals col-key)]
      (if (neg? col-id)
        not-found
        (let [payload-id (payload-id internals rowId col-id)]
          (if (= -1 payload-id)
            not-found
            (payload-at internals mode payload-id))))))

  clojure.lang.Associative
  (containsKey [_ col-key]
    (let [col-id (col-id internals col-key)]
      (and (not (neg? col-id))
           (not= -1 (payload-id internals rowId col-id)))))
  (entryAt [this col-key]
    (let [value (.valAt this col-key missing)]
      (when-not (identical? missing value)
        (MapEntry. col-key value))))
  (assoc [_ _ _]
    (unsupported-assoc))

  clojure.lang.Seqable
  (seq [_]
    (let [[start end] (row-span internals rowId)
          col-ids ^ints (:csr-col-ids internals)]
      (seq (map (fn [i]
                  (let [col-id (aget col-ids i)]
                    (MapEntry. (col-key internals col-id)
                               (payload-at internals mode i))))
                (range start end)))))

  clojure.lang.Counted
  (count [_]
    (let [[start end] (row-span internals rowId)]
      (- end start)))

  clojure.lang.IFn
  (invoke [this col-key]
    (.valAt this col-key))
  (invoke [this col-key not-found]
    (.valAt this col-key not-found))

  clojure.lang.IKVReduce
  (kvreduce [_ f init]
    (let [[start end] (row-span internals rowId)
          col-ids ^ints (:csr-col-ids internals)]
      (loop [ret init
             i start]
        (if (= i end)
          ret
          (let [col-id (aget col-ids i)
                ret (f ret
                       (col-key internals col-id)
                       (payload-at internals mode i))]
            (if (reduced? ret)
              @ret
              (recur ret (inc i))))))))

  Object
  (toString [_]
    (str "#<SparseRowMap row=" rowKey " count=" (count _) ">")))

(deftype ColumnMap [ds internals colKey ^long colId mode]
  clojure.lang.ILookup
  (valAt [this row-key]
    (.valAt this row-key nil))
  (valAt [_ row-key not-found]
    (let [row-id (row-id internals row-key)]
      (if (neg? row-id)
        not-found
        (let [payload-id (payload-id internals row-id colId)]
          (if (= -1 payload-id)
            not-found
            (payload-at internals mode payload-id))))))

  clojure.lang.Associative
  (containsKey [_ row-key]
    (let [row-id (row-id internals row-key)]
      (and (not (neg? row-id))
           (not= -1 (payload-id internals row-id colId)))))
  (entryAt [this row-key]
    (let [value (.valAt this row-key missing)]
      (when-not (identical? missing value)
        (MapEntry. row-key value))))
  (assoc [_ _ _]
    (unsupported-assoc))

  clojure.lang.Seqable
  (seq [_]
    (let [[start end] (col-span internals colId)
          row-ids ^ints (:csc-row-ids internals)
          payload-ids ^ints (:csc-payload-ids internals)]
      (seq (map (fn [i]
                  (let [row-id (aget row-ids i)
                        payload-id (aget payload-ids i)]
                    (MapEntry. (row-key internals row-id)
                               (payload-at internals mode payload-id))))
                (range start end)))))

  clojure.lang.Counted
  (count [_]
    (let [[start end] (col-span internals colId)]
      (- end start)))

  clojure.lang.IFn
  (invoke [this row-key]
    (.valAt this row-key))
  (invoke [this row-key not-found]
    (.valAt this row-key not-found))

  clojure.lang.IKVReduce
  (kvreduce [_ f init]
    (let [[start end] (col-span internals colId)
          row-ids ^ints (:csc-row-ids internals)
          payload-ids ^ints (:csc-payload-ids internals)]
      (loop [ret init
             i start]
        (if (= i end)
          ret
          (let [row-id (aget row-ids i)
                payload-id (aget payload-ids i)
                ret (f ret
                       (row-key internals row-id)
                       (payload-at internals mode payload-id))]
            (if (reduced? ret)
              @ret
              (recur ret (inc i))))))))

  Object
  (toString [_]
    (str "#<SparseColumnMap col=" colKey " count=" (count _) ">")))

(deftype DatasetMap [ds internals mode]
  clojure.lang.ILookup
  (valAt [this row-key]
    (.valAt this row-key nil))
  (valAt [_ row-key not-found]
    (let [row-id (row-id internals row-key)]
      (if (neg? row-id)
        not-found
        (RowMap. ds internals row-key row-id mode))))

  clojure.lang.Associative
  (containsKey [_ row-key]
    (not (neg? (row-id internals row-key))))
  (entryAt [this row-key]
    (let [value (.valAt this row-key missing)]
      (when-not (identical? missing value)
        (MapEntry. row-key value))))
  (assoc [_ _ _]
    (unsupported-assoc))

  clojure.lang.Seqable
  (seq [_]
    (let [rows ^objects (:id->row internals)
          n (alength rows)]
      (seq (map (fn [row-id]
                  (let [row-key (aget rows row-id)]
                    (MapEntry. row-key
                               (RowMap. ds internals row-key row-id mode))))
                (range n)))))

  clojure.lang.Counted
  (count [_]
    (alength ^objects (:id->row internals)))

  clojure.lang.IFn
  (invoke [this row-key]
    (.valAt this row-key))
  (invoke [this row-key not-found]
    (.valAt this row-key not-found))

  clojure.lang.IKVReduce
  (kvreduce [_ f init]
    (let [rows ^objects (:id->row internals)
          n (alength rows)]
      (loop [ret init
             row-id 0]
        (if (= row-id n)
          ret
          (let [row-key (aget rows row-id)
                ret (f ret
                       row-key
                       (RowMap. ds internals row-key row-id mode))]
            (if (reduced? ret)
              @ret
              (recur ret (inc row-id))))))))

  Object
  (toString [_]
    (str "#<SparseDatasetMap rows=" (count _) ">")))

(defn as-map
  "Returns a read-only map facade: row key -> row map -> copied payload value."
  [ds]
  (DatasetMap. ds (internals ds) :value))

(defn as-view-map
  "Returns a read-only map facade whose row values expose zero-copy block views."
  [ds]
  (DatasetMap. ds (internals ds) :view))

(defn row-map
  "Returns a read-only row map facade: column key -> copied payload value."
  [ds row-key]
  (let [internals (internals ds)
        row-id (row-id internals row-key)]
    (when-not (neg? row-id)
      (RowMap. ds internals row-key row-id :value))))

(defn row-view-map
  "Returns a read-only row map facade: column key -> zero-copy block view."
  [ds row-key]
  (let [internals (internals ds)
        row-id (row-id internals row-key)]
    (when-not (neg? row-id)
      (RowMap. ds internals row-key row-id :view))))

(defn col-map
  "Returns a read-only column map facade: row key -> copied payload value."
  [ds col-key]
  (let [internals (internals ds)
        col-id (col-id internals col-key)]
    (when-not (neg? col-id)
      (ColumnMap. ds internals col-key col-id :value))))

(defn col-view-map
  "Returns a read-only column map facade: row key -> zero-copy block view."
  [ds col-key]
  (let [internals (internals ds)
        col-id (col-id internals col-key)]
    (when-not (neg? col-id)
      (ColumnMap. ds internals col-key col-id :view))))
