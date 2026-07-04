(ns sparse-layout.facade
  (:require [sparse-layout.core :as sparse])
  (:import [clojure.lang MapEntry]))

(def ^:private missing (Object.))

(defn- unsupported-assoc
  []
  (throw
    (UnsupportedOperationException.
      "Sparse facades are immutable read views; rebuild through a sparse builder to add or remove entries.")))

;; Fields are extracted from the internals map once at construction and stored
;; directly on each deftype, so no map lookups occur in hot-path operations.

(deftype RowMap [ds rowKey ^long rowId mode ^java.util.Map colToId ^objects idToCol ^ints csrRowPtrs
                 ^ints csrColIds ^ints cscColPtrs ^ints cscRowIds ^ints cscPayloadIds payloadKind
                 ^long payloadDim payloadValues ^ints payloadPtrs]
  clojure.lang.ILookup
    (valAt [this col-key] (.valAt this col-key nil))
    (valAt [_ col-key not-found]
      (let [cid (sparse/id-of colToId col-key)]
        (if (neg? cid)
          not-found
          (let [pid (sparse/payload-id-by-ids rowId
                                              cid
                                              csrRowPtrs
                                              csrColIds
                                              cscColPtrs
                                              cscRowIds
                                              cscPayloadIds)]
            (if (= -1 pid)
              not-found
              (case mode
                :value
                (sparse/payload-value payloadKind payloadDim payloadValues payloadPtrs pid)

                :view
                (sparse/payload-view payloadKind payloadDim payloadValues payloadPtrs pid)))))))
  clojure.lang.Associative
    (containsKey [_ col-key]
      (let [cid (sparse/id-of colToId col-key)]
        (and (not (neg? cid))
             (not= -1
                   (sparse/payload-id-by-ids rowId
                                             cid
                                             csrRowPtrs
                                             csrColIds
                                             cscColPtrs
                                             cscRowIds
                                             cscPayloadIds)))))
    (entryAt [this col-key]
      (let [value (.valAt this col-key missing)]
        (when-not (identical? missing value) (MapEntry. col-key value))))
    (assoc [_ _ _] (unsupported-assoc))
  clojure.lang.Seqable
    (seq [_]
      (if-let [ptrs ^ints csrRowPtrs]
        (let [row-id (int rowId)
              start (aget ptrs row-id)
              end (aget ptrs (inc row-id))
              col-ids ^ints csrColIds
              id->col ^objects idToCol
              pk payloadKind
              pd payloadDim
              pv payloadValues
              pp payloadPtrs]

          (seq (map (fn [i]
                      (MapEntry. (sparse/key-of id->col (aget col-ids i))
                                 (case mode
                                   :value
                                   (sparse/payload-value pk pd pv pp i)

                                   :view
                                   (sparse/payload-view pk pd pv pp i))))
                    (range start end))))
        (throw (ex-info "CSR index is required for row facade traversal." {:required :csr}))))
  clojure.lang.Counted
    (count [_]
      (if-let [ptrs ^ints csrRowPtrs]
        (let [row-id (int rowId)]
          (- (aget ptrs (inc row-id)) (aget ptrs row-id)))
        (throw (ex-info "CSR index is required for row facade traversal." {:required :csr}))))
  clojure.lang.IFn
    (invoke [this col-key] (.valAt this col-key))
    (invoke [this col-key not-found] (.valAt this col-key not-found))
  clojure.lang.IKVReduce
    (kvreduce [_ f init]
      (if-let [ptrs ^ints csrRowPtrs]
        (let [row-id (int rowId)
              start (aget ptrs row-id)
              end (aget ptrs (inc row-id))
              col-ids ^ints csrColIds
              id->col ^objects idToCol
              pk payloadKind
              pd payloadDim
              pv payloadValues
              pp payloadPtrs]

          (loop [ret init
                 i start]

            (if (= i end)
              ret
              (let [ret (f ret
                           (sparse/key-of id->col (aget col-ids i))
                           (case mode
                             :value
                             (sparse/payload-value pk pd pv pp i)

                             :view
                             (sparse/payload-view pk pd pv pp i)))]
                (if (reduced? ret) @ret (recur ret (inc i)))))))
        (throw (ex-info "CSR index is required for row facade traversal." {:required :csr}))))
  Object
    (toString [_] (str "#<SparseRowMap row=" rowKey " count=" (count _) ">")))

(deftype ColumnMap [ds colKey ^long colId mode ^java.util.Map rowToId ^objects idToRow
                    ^ints csrRowPtrs ^ints csrColIds ^ints cscColPtrs ^ints cscRowIds
                    ^ints cscPayloadIds payloadKind ^long payloadDim payloadValues
                    ^ints payloadPtrs]
  clojure.lang.ILookup
    (valAt [this row-key] (.valAt this row-key nil))
    (valAt [_ row-key not-found]
      (let [rid (sparse/id-of rowToId row-key)]
        (if (neg? rid)
          not-found
          (let [pid (sparse/payload-id-by-ids rid
                                              colId
                                              csrRowPtrs
                                              csrColIds
                                              cscColPtrs
                                              cscRowIds
                                              cscPayloadIds)]
            (if (= -1 pid)
              not-found
              (case mode
                :value
                (sparse/payload-value payloadKind payloadDim payloadValues payloadPtrs pid)

                :view
                (sparse/payload-view payloadKind payloadDim payloadValues payloadPtrs pid)))))))
  clojure.lang.Associative
    (containsKey [_ row-key]
      (let [rid (sparse/id-of rowToId row-key)]
        (and (not (neg? rid))
             (not= -1
                   (sparse/payload-id-by-ids rid
                                             colId
                                             csrRowPtrs
                                             csrColIds
                                             cscColPtrs
                                             cscRowIds
                                             cscPayloadIds)))))
    (entryAt [this row-key]
      (let [value (.valAt this row-key missing)]
        (when-not (identical? missing value) (MapEntry. row-key value))))
    (assoc [_ _ _] (unsupported-assoc))
  clojure.lang.Seqable
    (seq [_]
      (if-let [ptrs ^ints cscColPtrs]
        (let [col-id (int colId)
              start (aget ptrs col-id)
              end (aget ptrs (inc col-id))
              row-ids ^ints cscRowIds
              payload-ids ^ints cscPayloadIds
              id->row ^objects idToRow
              pk payloadKind
              pd payloadDim
              pv payloadValues
              pp payloadPtrs]

          (seq (map (fn [i]
                      (MapEntry. (sparse/key-of id->row (aget row-ids i))
                                 (case mode
                                   :value
                                   (sparse/payload-value pk pd pv pp (aget payload-ids i))

                                   :view
                                   (sparse/payload-view pk pd pv pp (aget payload-ids i)))))
                    (range start end))))
        (throw (ex-info "CSC index is required for column facade traversal." {:required :csc}))))
  clojure.lang.Counted
    (count [_]
      (if-let [ptrs ^ints cscColPtrs]
        (let [col-id (int colId)]
          (- (aget ptrs (inc col-id)) (aget ptrs col-id)))
        (throw (ex-info "CSC index is required for column facade traversal." {:required :csc}))))
  clojure.lang.IFn
    (invoke [this row-key] (.valAt this row-key))
    (invoke [this row-key not-found] (.valAt this row-key not-found))
  clojure.lang.IKVReduce
    (kvreduce [_ f init]
      (if-let [ptrs ^ints cscColPtrs]
        (let [col-id (int colId)
              start (aget ptrs col-id)
              end (aget ptrs (inc col-id))
              row-ids ^ints cscRowIds
              payload-ids ^ints cscPayloadIds
              id->row ^objects idToRow
              pk payloadKind
              pd payloadDim
              pv payloadValues
              pp payloadPtrs]

          (loop [ret init
                 i start]

            (if (= i end)
              ret
              (let [ret (f ret
                           (sparse/key-of id->row (aget row-ids i))
                           (case mode
                             :value
                             (sparse/payload-value pk pd pv pp (aget payload-ids i))

                             :view
                             (sparse/payload-view pk pd pv pp (aget payload-ids i))))]
                (if (reduced? ret) @ret (recur ret (inc i)))))))
        (throw (ex-info "CSC index is required for column facade traversal." {:required :csc}))))
  Object
    (toString [_] (str "#<SparseColumnMap col=" colKey " count=" (count _) ">")))

(deftype DatasetMap [ds mode ^java.util.Map rowToId ^objects idToRow ^java.util.Map colToId
                     ^objects idToCol ^ints csrRowPtrs ^ints csrColIds ^ints cscColPtrs
                     ^ints cscRowIds ^ints cscPayloadIds payloadKind ^long payloadDim payloadValues
                     ^ints payloadPtrs]
  clojure.lang.ILookup
    (valAt [this row-key] (.valAt this row-key nil))
    (valAt [_ row-key not-found]
      (let [rid (sparse/id-of rowToId row-key)]
        (if (neg? rid)
          not-found
          (RowMap. ds
                   row-key
                   rid
                   mode
                   colToId
                   idToCol
                   csrRowPtrs
                   csrColIds
                   cscColPtrs
                   cscRowIds
                   cscPayloadIds
                   payloadKind
                   payloadDim
                   payloadValues
                   payloadPtrs))))
  clojure.lang.Associative
    (containsKey [_ row-key] (not (neg? (sparse/id-of rowToId row-key))))
    (entryAt [this row-key]
      (let [value (.valAt this row-key missing)]
        (when-not (identical? missing value) (MapEntry. row-key value))))
    (assoc [_ _ _] (unsupported-assoc))
  clojure.lang.Seqable
    (seq [_]
      (let [rows
            ^objects idToRow

            n
            (alength rows)]

        (seq
          (map (fn [row-id]
                 (let [rk (aget rows row-id)]
                   (MapEntry. rk
                              (RowMap. ds
                                       rk
                                       row-id
                                       mode
                                       colToId
                                       idToCol
                                       csrRowPtrs
                                       csrColIds
                                       cscColPtrs
                                       cscRowIds
                                       cscPayloadIds
                                       payloadKind
                                       payloadDim
                                       payloadValues
                                       payloadPtrs))))
               (range n)))))
  clojure.lang.Counted
    (count [_] (alength ^objects idToRow))
  clojure.lang.IFn
    (invoke [this row-key] (.valAt this row-key))
    (invoke [this row-key not-found] (.valAt this row-key not-found))
  clojure.lang.IKVReduce
    (kvreduce [_ f init]
      (let [rows
            ^objects idToRow

            n
            (alength rows)]

        (loop [ret
               init

               row-id
               0]

          (if (= row-id n)
            ret
            (let [rk
                  (aget rows row-id)

                  ret
                  (f ret
                     rk
                     (RowMap. ds
                              rk
                              row-id
                              mode
                              colToId
                              idToCol
                              csrRowPtrs
                              csrColIds
                              cscColPtrs
                              cscRowIds
                              cscPayloadIds
                              payloadKind
                              payloadDim
                              payloadValues
                              payloadPtrs))]

              (if (reduced? ret) @ret (recur ret (inc row-id))))))))
  Object
    (toString [_] (str "#<SparseDatasetMap rows=" (count _) ">")))

(defn- row-map*
  [ds m row-key row-id mode]
  (RowMap. ds
           row-key
           row-id
           mode
           (:col->id m)
           (:id->col m)
           (:csr-row-ptrs m)
           (:csr-col-ids m)
           (:csc-col-ptrs m)
           (:csc-row-ids m)
           (:csc-payload-ids m)
           (:payload-kind m)
           (:payload-dim m)
           (:payload-values m)
           (:payload-ptrs m)))

(defn- col-map*
  [ds m col-key col-id mode]
  (ColumnMap. ds
              col-key
              col-id
              mode
              (:row->id m)
              (:id->row m)
              (:csr-row-ptrs m)
              (:csr-col-ids m)
              (:csc-col-ptrs m)
              (:csc-row-ids m)
              (:csc-payload-ids m)
              (:payload-kind m)
              (:payload-dim m)
              (:payload-values m)
              (:payload-ptrs m)))

(defn- dataset-map*
  [ds m mode]
  (DatasetMap. ds
               mode
               (:row->id m)
               (:id->row m)
               (:col->id m)
               (:id->col m)
               (:csr-row-ptrs m)
               (:csr-col-ids m)
               (:csc-col-ptrs m)
               (:csc-row-ids m)
               (:csc-payload-ids m)
               (:payload-kind m)
               (:payload-dim m)
               (:payload-values m)
               (:payload-ptrs m)))

(defn as-map
  "Returns a read-only map facade: row key -> row map -> copied payload value."
  [ds]
  (dataset-map* ds (sparse/sparse-internals ds) :value))

(defn as-view-map
  "Returns a read-only map facade whose row values expose zero-copy block views."
  [ds]
  (dataset-map* ds (sparse/sparse-internals ds) :view))

(defn row-map
  "Returns a read-only row map facade: column key -> copied payload value."
  [ds row-key]
  (let [m
        (sparse/sparse-internals ds)

        rid
        (sparse/id-of (:row->id m) row-key)]

    (when-not (neg? rid) (row-map* ds m row-key rid :value))))

(defn row-view-map
  "Returns a read-only row map facade: column key -> zero-copy block view."
  [ds row-key]
  (let [m
        (sparse/sparse-internals ds)

        rid
        (sparse/id-of (:row->id m) row-key)]

    (when-not (neg? rid) (row-map* ds m row-key rid :view))))

(defn col-map
  "Returns a read-only column map facade: row key -> copied payload value."
  [ds col-key]
  (let [m
        (sparse/sparse-internals ds)

        cid
        (sparse/id-of (:col->id m) col-key)]

    (when-not (neg? cid) (col-map* ds m col-key cid :value))))

(defn col-view-map
  "Returns a read-only column map facade: row key -> zero-copy block view."
  [ds col-key]
  (let [m
        (sparse/sparse-internals ds)

        cid
        (sparse/id-of (:col->id m) col-key)]

    (when-not (neg? cid) (col-map* ds m col-key cid :view))))
