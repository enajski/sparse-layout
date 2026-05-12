(ns sparse-layout.core
  (:require [clojure.string :as str])
  (:import [java.util ArrayList Arrays Collections Comparator HashMap Map]))

(defprotocol SparseDataset
  (row-id [this row-key])
  (col-id [this col-key])
  (row-blocks [this row-key])
  (col-blocks [this col-key])
  (block [this row-key col-key]))

(defprotocol SparseBlockViews
  (row-block-views [this row-key])
  (col-block-views [this col-key])
  (block-view [this row-key col-key]))

(def ^:private allowed-payload-kinds
  #{:double :long :object :fixed-double-block :var-double-block})

(def ^:private allowed-duplicate-policies
  #{:last :sum :merge :error})

(def ^:private double-array-class
  (Class/forName "[D"))

(defn- numeric-seqable? [value]
  (and (not (nil? value))
       (not (map? value))
       (seqable? value)))

(defprotocol BufferAccess
  (buf-data [this])
  (buf-set-data! [this data])
  (buf-size [this])
  (buf-set-size! [this size]))

(deftype IntBuffer
  [^:unsynchronized-mutable ^ints data
   ^:unsynchronized-mutable ^int size]
  BufferAccess
  (buf-data [_] data)
  (buf-set-data! [_ new-data] (set! data new-data))
  (buf-size [_] size)
  (buf-set-size! [_ new-size] (set! size (int new-size))))

(deftype DoubleBuffer
  [^:unsynchronized-mutable ^doubles data
   ^:unsynchronized-mutable ^int size]
  BufferAccess
  (buf-data [_] data)
  (buf-set-data! [_ new-data] (set! data new-data))
  (buf-size [_] size)
  (buf-set-size! [_ new-size] (set! size (int new-size))))

(deftype LongBuffer
  [^:unsynchronized-mutable ^longs data
   ^:unsynchronized-mutable ^int size]
  BufferAccess
  (buf-data [_] data)
  (buf-set-data! [_ new-data] (set! data new-data))
  (buf-size [_] size)
  (buf-set-size! [_ new-size] (set! size (int new-size))))

(deftype ObjectBuffer
  [^:unsynchronized-mutable ^objects data
   ^:unsynchronized-mutable ^int size]
  BufferAccess
  (buf-data [_] data)
  (buf-set-data! [_ new-data] (set! data new-data))
  (buf-size [_] size)
  (buf-set-size! [_ new-size] (set! size (int new-size))))

(deftype DoubleBlockView
  [^doubles values
   ^int offset
   ^int length]
  Object
  (toString [_]
    (str "#<DoubleBlockView offset=" offset " length=" length ">")))

(defn- int-buffer
  (^IntBuffer [] (int-buffer 16))
  (^IntBuffer [capacity] (IntBuffer. (int-array (max 1 (int capacity))) 0)))

(defn- double-buffer
  (^DoubleBuffer [] (double-buffer 16))
  (^DoubleBuffer [capacity] (DoubleBuffer. (double-array (max 1 (int capacity))) 0)))

(defn- long-buffer
  (^LongBuffer [] (long-buffer 16))
  (^LongBuffer [capacity] (LongBuffer. (long-array (max 1 (int capacity))) 0)))

(defn- object-buffer
  (^ObjectBuffer [] (object-buffer 16))
  (^ObjectBuffer [capacity] (ObjectBuffer. (object-array (max 1 (int capacity))) 0)))

(defn- grow-capacity ^long [^long current ^long needed]
  (loop [capacity (max 1 current)]
    (if (>= capacity needed)
      capacity
      (recur (* 2 capacity)))))

(defn- ib-ensure! [^IntBuffer buffer ^long needed]
  (let [data ^ints (buf-data buffer)]
    (when (> needed (alength ^ints data))
      (let [new-data (int-array (grow-capacity (alength ^ints data) needed))]
        (System/arraycopy data 0 new-data 0 (buf-size buffer))
        (buf-set-data! buffer new-data)))))

(defn- db-ensure! [^DoubleBuffer buffer ^long needed]
  (let [data ^doubles (buf-data buffer)]
    (when (> needed (alength ^doubles data))
      (let [new-data (double-array (grow-capacity (alength ^doubles data) needed))]
        (System/arraycopy data 0 new-data 0 (buf-size buffer))
        (buf-set-data! buffer new-data)))))

(defn- lb-ensure! [^LongBuffer buffer ^long needed]
  (let [data ^longs (buf-data buffer)]
    (when (> needed (alength ^longs data))
      (let [new-data (long-array (grow-capacity (alength ^longs data) needed))]
        (System/arraycopy data 0 new-data 0 (buf-size buffer))
        (buf-set-data! buffer new-data)))))

(defn- ob-ensure! [^ObjectBuffer buffer ^long needed]
  (let [data ^objects (buf-data buffer)]
    (when (> needed (alength ^objects data))
      (let [new-data (object-array (grow-capacity (alength ^objects data) needed))]
        (System/arraycopy data 0 new-data 0 (buf-size buffer))
        (buf-set-data! buffer new-data)))))

(defn- ib-add! [^IntBuffer buffer ^long value]
  (let [size (buf-size buffer)]
    (ib-ensure! buffer (inc size))
    (aset-int ^ints (buf-data buffer) size (int value))
    (buf-set-size! buffer (inc size))
    buffer))

(defn- db-add! [^DoubleBuffer buffer ^double value]
  (let [size (buf-size buffer)]
    (db-ensure! buffer (inc size))
    (aset-double ^doubles (buf-data buffer) size value)
    (buf-set-size! buffer (inc size))
    buffer))

(defn- lb-add! [^LongBuffer buffer ^long value]
  (let [size (buf-size buffer)]
    (lb-ensure! buffer (inc size))
    (aset-long ^longs (buf-data buffer) size value)
    (buf-set-size! buffer (inc size))
    buffer))

(defn- ob-add! [^ObjectBuffer buffer value]
  (let [size (buf-size buffer)]
    (ob-ensure! buffer (inc size))
    (aset ^objects (buf-data buffer) size value)
    (buf-set-size! buffer (inc size))
    buffer))

(defn- ib-get ^long [^IntBuffer buffer ^long i]
  (aget ^ints (buf-data buffer) (int i)))

(defn- db-get ^double [^DoubleBuffer buffer ^long i]
  (aget ^doubles (buf-data buffer) (int i)))

(defn- lb-get ^long [^LongBuffer buffer ^long i]
  (aget ^longs (buf-data buffer) (int i)))

(defn- ob-get [^ObjectBuffer buffer ^long i]
  (aget ^objects (buf-data buffer) (int i)))

(defn- ib-size ^long [^IntBuffer buffer]
  (buf-size buffer))

(defn- db-size ^long [^DoubleBuffer buffer]
  (buf-size buffer))

(defn- ib->array ^ints [^IntBuffer buffer]
  (let [size (buf-size buffer)
        out (int-array size)]
    (System/arraycopy (buf-data buffer) 0 out 0 size)
    out))

(defn- db->array ^doubles [^DoubleBuffer buffer]
  (let [size (buf-size buffer)
        out (double-array size)]
    (System/arraycopy (buf-data buffer) 0 out 0 size)
    out))

(defn- lb->array ^longs [^LongBuffer buffer]
  (let [size (buf-size buffer)
        out (long-array size)]
    (System/arraycopy (buf-data buffer) 0 out 0 size)
    out))

(defn- ob->array ^objects [^ObjectBuffer buffer]
  (let [size (buf-size buffer)
        out (object-array size)]
    (System/arraycopy (buf-data buffer) 0 out 0 size)
    out))

(defn- ex [message data]
  (ex-info message data))

(defn- normalize-payload-spec [payload]
  (let [payload (cond
                  (nil? payload) {:kind :double}
                  (keyword? payload) {:kind payload}
                  (map? payload) payload
                  :else (throw (ex "Payload must be a keyword or map."
                                   {:payload payload})))
        kind (:kind payload)]
    (when-not (contains? allowed-payload-kinds kind)
      (throw (ex "Unsupported payload kind."
                 {:kind kind
                  :allowed allowed-payload-kinds})))
    (when (= kind :fixed-double-block)
      (when-not (pos-int? (:dim payload))
        (throw (ex "Fixed double blocks require a positive :dim."
                   {:payload payload}))))
    payload))

(defn- normalize-layout [layout]
  (let [layout (if (map? layout)
                 layout
                 (throw (ex "Layout must be a map." {:layout layout})))
        duplicate-policy (or (:duplicate-policy layout) :last)
        indices (set (or (:indices layout) #{:csr}))
        retain-coo? (if (contains? layout :retain-coo?)
                      (boolean (:retain-coo? layout))
                      true)]
    (when-not (contains? allowed-duplicate-policies duplicate-policy)
      (throw (ex "Unsupported duplicate policy."
                 {:duplicate-policy duplicate-policy
                  :allowed allowed-duplicate-policies})))
    (assoc layout
           :payload (normalize-payload-spec (:payload layout))
           :duplicate-policy duplicate-policy
           :indices indices
           :retain-coo? retain-coo?)))

(defn- double-array-length ^long [value]
  (cond
    (instance? double-array-class value) (alength ^doubles value)
    (numeric-seqable? value) (count value)
    :else -1))

(defn- copy-double-values! [dest offset value expected label]
  (let [dest ^doubles dest
        offset (long offset)
        expected (long expected)]
    (if (instance? double-array-class value)
      (let [value ^doubles value
            length (alength value)]
        (when-not (= expected length)
          (throw (ex "Unexpected double block length."
                     {:expected expected
                      :actual length
                      :label label})))
        (System/arraycopy value 0 dest (int offset) (int expected)))
      (if (numeric-seqable? value)
        (do
          (when-not (= expected (count value))
            (throw (ex "Unexpected double block length."
                       {:expected expected
                        :actual (count value)
                        :label label})))
          (loop [i 0
                 xs (seq value)]
            (when xs
              (aset-double dest (int (+ offset i)) (double (first xs)))
              (recur (inc i) (next xs)))))
        (throw (ex "Payload must be a double array or sequence of numbers."
                   {:value value
                    :label label}))))))

(defn- double-array-value ^doubles [value expected label]
  (let [actual (double-array-length value)]
    (when (neg? actual)
      (throw (ex "Payload must be a double array or sequence of numbers."
                 {:value value
                  :label label})))
    (when (and expected (not= (long expected) actual))
      (throw (ex "Unexpected double block length."
                 {:expected expected
                  :actual actual
                  :label label})))
    (let [out (double-array actual)]
      (copy-double-values! out 0 value actual label)
      out)))

(defn- append-fixed-double-block! [^DoubleBuffer values ^long dim value]
  (let [start (db-size values)]
    (db-ensure! values (+ start dim))
    (copy-double-values! (buf-data values) start value dim :fixed-double-block)
    (buf-set-size! values (+ start dim))))

(defn- append-var-double-block! [^DoubleBuffer values value]
  (cond
    (instance? double-array-class value)
      (let [value ^doubles value
          length (alength value)
          start (db-size values)]
      (db-ensure! values (+ start length))
      (System/arraycopy value 0 (buf-data values) start length)
      (buf-set-size! values (+ start length)))

    (numeric-seqable? value)
    (doseq [x value]
      (db-add! values (double x)))

    :else
    (throw (ex "Variable double blocks require a double array or sequence."
               {:value value}))))

(defprotocol PayloadBuffer
  (-append-payload! [this value])
  (-payload-at [this edge-id])
  (-payload-count [this])
  (-payload-kind [this])
  (-payload-dim [this]))

(deftype DoublePayloadBuffer [^DoubleBuffer values]
  PayloadBuffer
  (-append-payload! [_ value]
    (db-add! values (double value)))
  (-payload-at [_ edge-id]
    (db-get values edge-id))
  (-payload-count [_]
    (db-size values))
  (-payload-kind [_]
    :double)
  (-payload-dim [_]
    1))

(deftype LongPayloadBuffer [^LongBuffer values]
  PayloadBuffer
  (-append-payload! [_ value]
    (lb-add! values (long value)))
  (-payload-at [_ edge-id]
    (lb-get values edge-id))
  (-payload-count [_]
    (buf-size values))
  (-payload-kind [_]
    :long)
  (-payload-dim [_]
    1))

(deftype ObjectPayloadBuffer [^ObjectBuffer values]
  PayloadBuffer
  (-append-payload! [_ value]
    (ob-add! values value))
  (-payload-at [_ edge-id]
    (ob-get values edge-id))
  (-payload-count [_]
    (buf-size values))
  (-payload-kind [_]
    :object)
  (-payload-dim [_]
    1))

(deftype FixedDoubleBlockPayloadBuffer [^long dim ^DoubleBuffer values ^IntBuffer count]
  PayloadBuffer
  (-append-payload! [_ value]
    (append-fixed-double-block! values dim value)
    (ib-add! count 1))
  (-payload-at [_ edge-id]
    (let [out (double-array dim)
          start (* (long edge-id) dim)]
      (System/arraycopy (buf-data values) (int start) out 0 (int dim))
      out))
  (-payload-count [_]
    (ib-size count))
  (-payload-kind [_]
    :fixed-double-block)
  (-payload-dim [_]
    dim))

(deftype VarDoubleBlockPayloadBuffer [^IntBuffer ptrs ^DoubleBuffer values]
  PayloadBuffer
  (-append-payload! [_ value]
    (append-var-double-block! values value)
    (ib-add! ptrs (db-size values)))
  (-payload-at [_ edge-id]
    (let [start (ib-get ptrs edge-id)
          end (ib-get ptrs (inc edge-id))
          length (- end start)
          out (double-array length)]
      (System/arraycopy (buf-data values) (int start) out 0 (int length))
      out))
  (-payload-count [_]
    (dec (ib-size ptrs)))
  (-payload-kind [_]
    :var-double-block)
  (-payload-dim [_]
    -1))

(defn- payload-buffer [payload]
  (case (:kind payload)
    :double (DoublePayloadBuffer. (double-buffer))
    :long (LongPayloadBuffer. (long-buffer))
    :object (ObjectPayloadBuffer. (object-buffer))
    :fixed-double-block (FixedDoubleBlockPayloadBuffer.
                         (long (:dim payload))
                         (double-buffer)
                         (int-buffer))
    :var-double-block (let [ptrs (int-buffer)]
                        (ib-add! ptrs 0)
                        (VarDoubleBlockPayloadBuffer. ptrs (double-buffer)))))

(deftype SparseBuilder
  [layout
   ^HashMap rowToId
   ^ArrayList idToRow
   ^HashMap colToId
   ^ArrayList idToCol
   ^IntBuffer rows
   ^IntBuffer cols
   payloadBuffer])

(defn make-builder
  "Creates a mutable sparse ingest builder for a normalized or declarative layout."
  ^SparseBuilder
  [layout]
  (let [layout (normalize-layout layout)]
    (SparseBuilder. layout
                    (HashMap.)
                    (ArrayList.)
                    (HashMap.)
                    (ArrayList.)
                    (int-buffer)
                    (int-buffer)
                    (payload-buffer (:payload layout)))))

(defn- intern-id! ^long [^HashMap forward ^ArrayList reverse key]
  (if (.containsKey forward key)
    (int (.get forward key))
    (let [id (.size reverse)]
      (.put forward key (int id))
      (.add reverse key)
      id)))

(defn append-entry!
  "Appends one COO edge into a mutable sparse builder."
  [^SparseBuilder builder row-key col-key payload]
  (let [row-id (intern-id! (.-rowToId builder) (.-idToRow builder) row-key)
        col-id (intern-id! (.-colToId builder) (.-idToCol builder) col-key)]
    (ib-add! (.-rows builder) row-id)
    (ib-add! (.-cols builder) col-id)
    (-append-payload! (.-payloadBuffer builder) payload)
    builder))

(defn- edge-order ^objects [^ints rows ^ints cols ^long n]
  (let [ids (object-array n)]
    (dotimes [i n]
      (aset ids i (Integer/valueOf i)))
    (Arrays/sort
     ids
     (reify Comparator
       (compare [_ a b]
         (let [ai (int a)
               bi (int b)
               row-compare (Integer/compare (aget rows ai) (aget rows bi))]
           (if (zero? row-compare)
             (let [col-compare (Integer/compare (aget cols ai) (aget cols bi))]
               (if (zero? col-compare)
                 (Integer/compare ai bi)
                 col-compare))
             row-compare)))))
    ids))

(defn- sum-double-values ^doubles [left right expected]
  (let [left (double-array-value left expected :sum-left)
        right (double-array-value right expected :sum-right)
        length (alength left)
        out (double-array length)]
    (dotimes [i length]
      (aset-double out i (+ (aget left i) (aget right i))))
    out))

(defn- sum-payload [payload left right]
  (case (:kind payload)
    :double (+ (double left) (double right))
    :long (+ (long left) (long right))
    :fixed-double-block (sum-double-values left right (:dim payload))
    :var-double-block (let [left-length (double-array-length left)
                            right-length (double-array-length right)]
                        (when-not (= left-length right-length)
                          (throw (ex "Cannot sum variable blocks with different lengths."
                                     {:left-length left-length
                                      :right-length right-length})))
                        (sum-double-values left right left-length))
    :object (throw (ex "Cannot :sum object payloads."
                       {:payload payload}))))

(defn- combine-duplicate [layout left right row-id col-id]
  (case (:duplicate-policy layout)
    :last right
    :sum (sum-payload (:payload layout) left right)
    :merge (if-let [merge-fn (:merge-fn layout)]
             (merge-fn left right)
             (throw (ex "Duplicate policy :merge requires :merge-fn."
                        {:row-id row-id
                         :col-id col-id})))
    :error (throw (ex "Duplicate sparse coordinate."
                      {:row-id row-id
                       :col-id col-id}))))

(defn- coalesce-builder [^SparseBuilder builder]
  (let [rows (ib->array (.-rows builder))
        cols (ib->array (.-cols builder))
        payload-buffer (.-payloadBuffer builder)
        n (alength rows)]
    (when-not (= n (-payload-count payload-buffer))
      (throw (ex "COO coordinate and payload counts diverged."
                 {:coordinates n
                  :payloads (-payload-count payload-buffer)})))
    (let [order (edge-order rows cols n)
          out-rows (int-buffer n)
          out-cols (int-buffer n)
          out-payloads (ArrayList.)]
      (loop [pos 0
             have-current? false
             current-row -1
             current-col -1
             current-payload nil]
        (if (= pos n)
          (do
            (when have-current?
              (ib-add! out-rows current-row)
              (ib-add! out-cols current-col)
              (.add out-payloads current-payload))
            {:rows (ib->array out-rows)
             :cols (ib->array out-cols)
             :payloads out-payloads})
          (let [edge-id (int (aget order pos))
                row (aget rows edge-id)
                col (aget cols edge-id)
                payload (-payload-at payload-buffer edge-id)]
            (if-not have-current?
              (recur (inc pos) true row col payload)
              (if (and (= current-row row) (= current-col col))
                (recur (inc pos)
                       true
                       current-row
                       current-col
                       (combine-duplicate (.-layout builder)
                                          current-payload
                                          payload
                                          row
                                          col))
                (do
                  (ib-add! out-rows current-row)
                  (ib-add! out-cols current-col)
                  (.add out-payloads current-payload)
                  (recur (inc pos) true row col payload))))))))))

(defn- compile-payload-storage [payload ^ArrayList payloads]
  (let [n (.size payloads)]
    (case (:kind payload)
      :double
      (let [values (double-array n)]
        (dotimes [i n]
          (aset-double values i (double (.get payloads i))))
        {:payload-values values
         :payload-ptrs nil})

      :long
      (let [values (long-array n)]
        (dotimes [i n]
          (aset-long values i (long (.get payloads i))))
        {:payload-values values
         :payload-ptrs nil})

      :object
      (let [values (object-array n)]
        (dotimes [i n]
          (aset values i (.get payloads i)))
        {:payload-values values
         :payload-ptrs nil})

      :fixed-double-block
      (let [dim (long (:dim payload))
            values (double-array (* n dim))]
        (dotimes [i n]
          (copy-double-values! values (* i dim) (.get payloads i) dim :fixed-double-block))
        {:payload-values values
         :payload-ptrs nil})

      :var-double-block
      (let [ptrs (int-array (inc n))
            values (double-buffer)]
        (dotimes [i n]
          (append-var-double-block! values (.get payloads i))
          (aset-int ptrs (inc i) (int (db-size values))))
        {:payload-values (db->array values)
         :payload-ptrs ptrs}))))

(defn- build-csr [indices ^ints edge-rows ^ints edge-cols row-count nnz]
  (when (contains? indices :csr)
    (let [row-ptrs (int-array (inc row-count))
          col-ids (int-array nnz)]
      (dotimes [payload-id nnz]
        (let [row (aget edge-rows payload-id)]
          (aset-int row-ptrs (inc row) (inc (aget row-ptrs (inc row))))))
      (dotimes [row row-count]
        (aset-int row-ptrs (inc row) (+ (aget row-ptrs row)
                                        (aget row-ptrs (inc row)))))
      (dotimes [payload-id nnz]
        (aset-int col-ids payload-id (aget edge-cols payload-id)))
      {:csr-row-ptrs row-ptrs
       :csr-col-ids col-ids})))

(defn- build-csc [indices ^ints edge-rows ^ints edge-cols col-count nnz]
  (when (contains? indices :csc)
    (let [col-ptrs (int-array (inc col-count))
          ids (object-array nnz)
          row-ids (int-array nnz)
          payload-ids (int-array nnz)]
      (dotimes [payload-id nnz]
        (let [col (aget edge-cols payload-id)]
          (aset-int col-ptrs (inc col) (inc (aget col-ptrs (inc col))))
          (aset ids payload-id (Integer/valueOf payload-id))))
      (dotimes [col col-count]
        (aset-int col-ptrs (inc col) (+ (aget col-ptrs col)
                                        (aget col-ptrs (inc col)))))
      (Arrays/sort
       ids
       (reify Comparator
         (compare [_ a b]
           (let [ai (int a)
                 bi (int b)
                 col-compare (Integer/compare (aget edge-cols ai) (aget edge-cols bi))]
             (if (zero? col-compare)
               (Integer/compare (aget edge-rows ai) (aget edge-rows bi))
               col-compare)))))
      (dotimes [i nnz]
        (let [payload-id (int (aget ids i))]
          (aset-int row-ids i (aget edge-rows payload-id))
          (aset-int payload-ids i payload-id)))
      {:csc-col-ptrs col-ptrs
       :csc-row-ids row-ids
       :csc-payload-ids payload-ids})))

(defn- frozen-map ^Map [^HashMap source]
  (Collections/unmodifiableMap (HashMap. source)))

(defn freeze-builder!
  "Compiles a mutable COO builder into frozen primitive arrays and dictionaries.

  The returned map is intended for code generated by `defsparse`."
  [^SparseBuilder builder]
  (let [layout (.-layout builder)
        payload (:payload layout)
        row-count (.size ^ArrayList (.-idToRow builder))
        col-count (.size ^ArrayList (.-idToCol builder))
        coalesced (coalesce-builder builder)
        edge-rows ^ints (:rows coalesced)
        edge-cols ^ints (:cols coalesced)
        payloads ^ArrayList (:payloads coalesced)
        nnz (alength edge-rows)
        payload-storage (compile-payload-storage payload payloads)
        csr (or (build-csr (:indices layout) edge-rows edge-cols row-count nnz) {})
        csc (or (build-csc (:indices layout) edge-rows edge-cols col-count nnz) {})]
    (merge {:row->id (frozen-map (.-rowToId builder))
            :id->row (.toArray ^ArrayList (.-idToRow builder))
            :col->id (frozen-map (.-colToId builder))
            :id->col (.toArray ^ArrayList (.-idToCol builder))
            :edge-rows (when (:retain-coo? layout) edge-rows)
            :edge-cols (when (:retain-coo? layout) edge-cols)
            :payload-kind (:kind payload)
            :payload-dim (long (or (:dim payload) -1))}
           payload-storage
           csr
           csc)))

(defn id-of ^long [^Map id-map key]
  (let [value (.get id-map key)]
    (if (nil? value)
      -1
      (int value))))

(defn key-of [^objects keys ^long id]
  (when (and (<= 0 id) (< id (alength keys)))
    (aget keys (int id))))

(defn- find-payload-in-csr
  [csr-col-ids csr-row-ptrs row-id col-id]
  (if (nil? csr-row-ptrs)
    -1
    (let [csr-col-ids ^ints csr-col-ids
          row-ptrs ^ints csr-row-ptrs
          row-id (long row-id)
          col-id (long col-id)]
      (loop [lo (aget row-ptrs (int row-id))
             hi (dec (aget row-ptrs (inc (int row-id))))]
        (if (> lo hi)
          -1
          (let [mid (quot (+ lo hi) 2)
                edge-col (aget csr-col-ids mid)]
            (cond
              (= edge-col col-id) mid
              (< edge-col col-id) (recur (inc mid) hi)
              :else (recur lo (dec mid)))))))))

(defn- find-payload-in-csc
  [csc-row-ids csc-col-ptrs csc-payload-ids row-id col-id]
  (if (nil? csc-col-ptrs)
    -1
    (let [csc-row-ids ^ints csc-row-ids
          col-ptrs ^ints csc-col-ptrs
          csc-payload-ids ^ints csc-payload-ids
          row-id (long row-id)
          col-id (long col-id)]
      (loop [lo (aget col-ptrs (int col-id))
             hi (dec (aget col-ptrs (inc (int col-id))))]
        (if (> lo hi)
          -1
          (let [mid (quot (+ lo hi) 2)
                edge-row (aget csc-row-ids mid)]
            (cond
              (= edge-row row-id) (aget csc-payload-ids mid)
              (< edge-row row-id) (recur (inc mid) hi)
              :else (recur lo (dec mid)))))))))

(defn payload-value [payload-kind payload-dim payload-values payload-ptrs payload-id]
  (case payload-kind
    :double
    (let [values ^doubles payload-values
          payload-id (long payload-id)]
      (aget values (int payload-id)))

    :long
    (let [values ^longs payload-values
          payload-id (long payload-id)]
      (aget values (int payload-id)))

    :object
    (let [values ^objects payload-values
          payload-id (long payload-id)]
      (aget values (int payload-id)))

    :fixed-double-block
    (let [values ^doubles payload-values
          payload-id (long payload-id)
          payload-dim (long payload-dim)
          out (double-array payload-dim)
          start (* payload-id payload-dim)]
      (System/arraycopy values (int start) out 0 (int payload-dim))
      out)

    :var-double-block
    (let [values ^doubles payload-values
          payload-ptrs ^ints payload-ptrs
          payload-id (long payload-id)
          start (aget payload-ptrs (int payload-id))
          end (aget payload-ptrs (inc (int payload-id)))
          length (- end start)
          out (double-array length)]
      (System/arraycopy values start out 0 length)
      out)))

(defn payload-view [payload-kind payload-dim payload-values payload-ptrs payload-id]
  (case payload-kind
    :fixed-double-block
    (let [values ^doubles payload-values
          payload-id (long payload-id)
          payload-dim (long payload-dim)
          start (* payload-id payload-dim)]
      (DoubleBlockView. values (int start) (int payload-dim)))

    :var-double-block
    (let [values ^doubles payload-values
          payload-ptrs ^ints payload-ptrs
          payload-id (long payload-id)
          start (aget payload-ptrs (int payload-id))
          end (aget payload-ptrs (inc (int payload-id)))]
      (DoubleBlockView. values start (- end start)))

    (throw (ex "Zero-copy block views require block payload storage."
               {:payload-kind payload-kind}))))

(defn block-view-array ^doubles [^DoubleBlockView view]
  (.-values view))

(defn block-view-offset ^long [^DoubleBlockView view]
  (.-offset view))

(defn block-view-length ^long [^DoubleBlockView view]
  (.-length view))

(defn block-view-value ^double [^DoubleBlockView view ^long i]
  (let [offset (.-offset view)
        length (.-length view)]
    (when-not (and (<= 0 i) (< i length))
      (throw (ex "Block view index out of bounds."
                 {:index i
                  :length length})))
    (aget ^doubles (.-values view) (int (+ offset i)))))

(defn block-view->array ^doubles [^DoubleBlockView view]
  (let [length (.-length view)
        out (double-array length)]
    (System/arraycopy (.-values view) (.-offset view) out 0 length)
    out))

(defn block-view->vec [^DoubleBlockView view]
  (let [values ^doubles (.-values view)
        offset (.-offset view)
        length (.-length view)]
    (loop [i 0
           out (transient [])]
      (if (= i length)
        (persistent! out)
        (recur (inc i)
               (conj! out (aget values (+ offset i))))))))

(defn block-by-ids
  [row-id
   col-id
   csr-row-ptrs
   csr-col-ids
   csc-col-ptrs
   csc-row-ids
   csc-payload-ids
   payload-kind
   payload-dim
   payload-values
   payload-ptrs]
  (when (and (<= 0 row-id) (<= 0 col-id))
    (when (and (nil? csr-row-ptrs) (nil? csc-col-ptrs))
      (throw (ex "Neither CSR nor CSC index was compiled."
                 {:required #{:csr :csc}})))
    (let [payload-id (if (some? csr-row-ptrs)
                       (find-payload-in-csr csr-col-ids csr-row-ptrs row-id col-id)
                       (find-payload-in-csc csc-row-ids csc-col-ptrs csc-payload-ids row-id col-id))]
      (when-not (= -1 payload-id)
        (payload-value payload-kind payload-dim payload-values payload-ptrs payload-id)))))

(defn block-view-by-ids
  [row-id
   col-id
   csr-row-ptrs
   csr-col-ids
   csc-col-ptrs
   csc-row-ids
   csc-payload-ids
   payload-kind
   payload-dim
   payload-values
   payload-ptrs]
  (when (and (<= 0 row-id) (<= 0 col-id))
    (when (and (nil? csr-row-ptrs) (nil? csc-col-ptrs))
      (throw (ex "Neither CSR nor CSC index was compiled."
                 {:required #{:csr :csc}})))
    (let [payload-id (if (some? csr-row-ptrs)
                       (find-payload-in-csr csr-col-ids csr-row-ptrs row-id col-id)
                       (find-payload-in-csc csc-row-ids csc-col-ptrs csc-payload-ids row-id col-id))]
      (when-not (= -1 payload-id)
        (payload-view payload-kind payload-dim payload-values payload-ptrs payload-id)))))

(defn row-blocks-by-id
  [row-id
   id-to-col
   csr-col-ids
   csr-row-ptrs
   payload-kind
   payload-dim
   payload-values
   payload-ptrs]
  (cond
    (neg? row-id)
    []

    (nil? csr-row-ptrs)
    (throw (ex "CSR index was not compiled for row traversal."
               {:required :csr}))

    :else
    (let [id-to-col ^objects id-to-col
          csr-col-ids ^ints csr-col-ids
          csr-row-ptrs ^ints csr-row-ptrs
          row-id (long row-id)
          start (aget csr-row-ptrs (int row-id))
          end (aget csr-row-ptrs (inc (int row-id)))]
      (loop [i start
             out (transient [])]
        (if (= i end)
          (persistent! out)
          (let [payload-id i
                col-id (aget csr-col-ids i)]
            (recur (inc i)
                   (conj! out [(aget id-to-col col-id)
                               (payload-value payload-kind
                                              payload-dim
                                              payload-values
                                              payload-ptrs
                                              payload-id)]))))))))

(defn row-block-views-by-id
  [row-id
   id-to-col
   csr-col-ids
   csr-row-ptrs
   payload-kind
   payload-dim
   payload-values
   payload-ptrs]
  (cond
    (neg? row-id)
    []

    (nil? csr-row-ptrs)
    (throw (ex "CSR index was not compiled for row traversal."
               {:required :csr}))

    :else
    (let [id-to-col ^objects id-to-col
          csr-col-ids ^ints csr-col-ids
          csr-row-ptrs ^ints csr-row-ptrs
          row-id (long row-id)
          start (aget csr-row-ptrs (int row-id))
          end (aget csr-row-ptrs (inc (int row-id)))]
      (loop [i start
             out (transient [])]
        (if (= i end)
          (persistent! out)
          (let [payload-id i
                col-id (aget csr-col-ids i)]
            (recur (inc i)
                   (conj! out [(aget id-to-col col-id)
                               (payload-view payload-kind
                                             payload-dim
                                             payload-values
                                             payload-ptrs
                                             payload-id)]))))))))

(defn col-blocks-by-id
  [col-id
   id-to-row
   csc-row-ids
   csc-col-ptrs
   csc-payload-ids
   payload-kind
   payload-dim
   payload-values
   payload-ptrs]
  (cond
    (neg? col-id)
    []

    (nil? csc-col-ptrs)
    (throw (ex "CSC index was not compiled for column traversal."
               {:required :csc}))

    :else
    (let [id-to-row ^objects id-to-row
          csc-row-ids ^ints csc-row-ids
          csc-col-ptrs ^ints csc-col-ptrs
          csc-payload-ids ^ints csc-payload-ids
          col-id (long col-id)
          start (aget csc-col-ptrs (int col-id))
          end (aget csc-col-ptrs (inc (int col-id)))]
      (loop [i start
             out (transient [])]
        (if (= i end)
          (persistent! out)
          (let [payload-id (aget csc-payload-ids i)
                row-id (aget csc-row-ids i)]
            (recur (inc i)
                   (conj! out [(aget id-to-row row-id)
                               (payload-value payload-kind
                                              payload-dim
                                              payload-values
                                              payload-ptrs
                                              payload-id)]))))))))

(defn col-block-views-by-id
  [col-id
   id-to-row
   csc-row-ids
   csc-col-ptrs
   csc-payload-ids
   payload-kind
   payload-dim
   payload-values
   payload-ptrs]
  (cond
    (neg? col-id)
    []

    (nil? csc-col-ptrs)
    (throw (ex "CSC index was not compiled for column traversal."
               {:required :csc}))

    :else
    (let [id-to-row ^objects id-to-row
          csc-row-ids ^ints csc-row-ids
          csc-col-ptrs ^ints csc-col-ptrs
          csc-payload-ids ^ints csc-payload-ids
          col-id (long col-id)
          start (aget csc-col-ptrs (int col-id))
          end (aget csc-col-ptrs (inc (int col-id)))]
      (loop [i start
             out (transient [])]
        (if (= i end)
          (persistent! out)
          (let [payload-id (aget csc-payload-ids i)
                row-id (aget csc-row-ids i)]
            (recur (inc i)
                   (conj! out [(aget id-to-row row-id)
                               (payload-view payload-kind
                                             payload-dim
                                             payload-values
                                             payload-ptrs
                                             payload-id)]))))))))

(defn- pascal-case [value]
  (->> (str/split (name value) #"-")
       (remove str/blank?)
       (map str/capitalize)
       (apply str)))

(defn- path-form [root path]
  (reduce (fn [form key]
            `(get ~form ~key))
          root
          path))

(defmacro defsparse
  "Defines a sparse layout compiler.

  Expansion emits:
  - a layout var named `<name>-layout`
  - a marker protocol named `Sparse<Name>`
  - a frozen primitive-array-backed deftype named `<Name>`
  - builder, ingest, freeze, compile, and query functions prefixed with `<name>-`"
  [layout-name layout]
  (let [base-name (name layout-name)
        type-name (symbol (pascal-case layout-name))
        marker-protocol-name (symbol (str "Sparse" (pascal-case layout-name)))
        layout-var (symbol (str base-name "-layout"))
        make-builder-name (symbol (str "make-" base-name "-builder"))
        ingest-name (symbol (str base-name "-ingest!"))
        freeze-name (symbol (str base-name "-freeze!"))
        compile-name (symbol (str base-name "-compile"))
        row-id-name (symbol (str base-name "-row-id"))
        col-id-name (symbol (str base-name "-col-id"))
        row-name (symbol (str base-name "-row"))
        col-name (symbol (str base-name "-col"))
        block-name (symbol (str base-name "-block"))
        row-views-name (symbol (str base-name "-row-views"))
        col-views-name (symbol (str base-name "-col-views"))
        block-view-name (symbol (str base-name "-block-view"))
        marker-method-name (symbol (str base-name "-dataset?"))
        record-sym (gensym "record")
        ds-sym (with-meta (gensym "ds") {:tag type-name})
        builder-sym (gensym "builder")
        row-path (:row-key layout)
        cols-path (:cols-path layout)
        row-expr (path-form record-sym row-path)
        cols-expr (path-form record-sym cols-path)]
    `(do
       (def ~layout-var ~layout)

       (defprotocol ~marker-protocol-name
         (~marker-method-name [~'this]))

       (deftype ~type-name
         [^java.util.Map ~'rowToId
          ^objects ~'idToRow
          ^java.util.Map ~'colToId
          ^objects ~'idToCol
          ^ints ~'edgeRows
          ^ints ~'edgeCols
          ^ints ~'csrRowPtrs
          ^ints ~'csrColIds
          ^ints ~'cscColPtrs
          ^ints ~'cscRowIds
          ^ints ~'cscPayloadIds
          ~'payloadKind
          ^long ~'payloadDim
          ~'payloadValues
          ^ints ~'payloadPtrs]
         sparse-layout.core/SparseDataset
         (~'row-id [~'this row-key#]
           (sparse-layout.core/id-of ~'rowToId row-key#))
         (~'col-id [~'this col-key#]
           (sparse-layout.core/id-of ~'colToId col-key#))
         (~'row-blocks [~'this row-key#]
           (let [row-id# (sparse-layout.core/id-of ~'rowToId row-key#)]
             (sparse-layout.core/row-blocks-by-id row-id#
                                                  ~'idToCol
                                                  ~'csrColIds
                                                  ~'csrRowPtrs
                                                  ~'payloadKind
                                                  ~'payloadDim
                                                  ~'payloadValues
                                                  ~'payloadPtrs)))
         (~'col-blocks [~'this col-key#]
           (let [col-id# (sparse-layout.core/id-of ~'colToId col-key#)]
             (sparse-layout.core/col-blocks-by-id col-id#
                                                  ~'idToRow
                                                  ~'cscRowIds
                                                  ~'cscColPtrs
                                                  ~'cscPayloadIds
                                                  ~'payloadKind
                                                  ~'payloadDim
                                                  ~'payloadValues
                                                  ~'payloadPtrs)))
         (~'block [~'this row-key# col-key#]
           (let [row-id# (sparse-layout.core/id-of ~'rowToId row-key#)
                 col-id# (sparse-layout.core/id-of ~'colToId col-key#)]
             (sparse-layout.core/block-by-ids row-id#
                                              col-id#
                                              ~'csrRowPtrs
                                              ~'csrColIds
                                              ~'cscColPtrs
                                              ~'cscRowIds
                                              ~'cscPayloadIds
                                              ~'payloadKind
                                              ~'payloadDim
                                              ~'payloadValues
                                              ~'payloadPtrs)))
         sparse-layout.core/SparseBlockViews
         (~'row-block-views [~'this row-key#]
           (let [row-id# (sparse-layout.core/id-of ~'rowToId row-key#)]
             (sparse-layout.core/row-block-views-by-id row-id#
                                                       ~'idToCol
                                                       ~'csrColIds
                                                       ~'csrRowPtrs
                                                       ~'payloadKind
                                                       ~'payloadDim
                                                       ~'payloadValues
                                                       ~'payloadPtrs)))
         (~'col-block-views [~'this col-key#]
           (let [col-id# (sparse-layout.core/id-of ~'colToId col-key#)]
             (sparse-layout.core/col-block-views-by-id col-id#
                                                       ~'idToRow
                                                       ~'cscRowIds
                                                       ~'cscColPtrs
                                                       ~'cscPayloadIds
                                                       ~'payloadKind
                                                       ~'payloadDim
                                                       ~'payloadValues
                                                       ~'payloadPtrs)))
         (~'block-view [~'this row-key# col-key#]
           (let [row-id# (sparse-layout.core/id-of ~'rowToId row-key#)
                 col-id# (sparse-layout.core/id-of ~'colToId col-key#)]
             (sparse-layout.core/block-view-by-ids row-id#
                                                   col-id#
                                                   ~'csrRowPtrs
                                                   ~'csrColIds
                                                   ~'cscColPtrs
                                                   ~'cscRowIds
                                                   ~'cscPayloadIds
                                                   ~'payloadKind
                                                   ~'payloadDim
                                                   ~'payloadValues
                                                   ~'payloadPtrs)))
         ~marker-protocol-name
         (~marker-method-name [~'this] true)
         Object
         (~'toString [~'this]
           (str "#<" '~type-name
                " rows=" (alength ~'idToRow)
                " cols=" (alength ~'idToCol)
                " nnz=" (cond
                          (some? ~'csrColIds) (alength ~'csrColIds)
                          (some? ~'cscRowIds) (alength ~'cscRowIds)
                          (some? ~'edgeRows) (alength ~'edgeRows)
                          :else 0)
                ">")))

       (defn ~make-builder-name
         ([] (sparse-layout.core/make-builder ~layout-var))
         ([opts#] (sparse-layout.core/make-builder (merge ~layout-var opts#))))

       (defn ~ingest-name
         [~builder-sym ~record-sym]
         (let [row-key# ~row-expr
               cols# ~cols-expr]
           (doseq [[col-key# payload#] cols#]
             (sparse-layout.core/append-entry! ~builder-sym row-key# col-key# payload#))
           ~builder-sym))

       (defn ~freeze-name
         [~builder-sym]
         (let [compiled# (sparse-layout.core/freeze-builder! ~builder-sym)]
           (new ~type-name
                (:row->id compiled#)
                (:id->row compiled#)
                (:col->id compiled#)
                (:id->col compiled#)
                (:edge-rows compiled#)
                (:edge-cols compiled#)
                (:csr-row-ptrs compiled#)
                (:csr-col-ids compiled#)
                (:csc-col-ptrs compiled#)
                (:csc-row-ids compiled#)
                (:csc-payload-ids compiled#)
                (:payload-kind compiled#)
                (:payload-dim compiled#)
                (:payload-values compiled#)
                (:payload-ptrs compiled#))))

       (defn ~compile-name
         [records#]
         (let [builder# (~make-builder-name)]
           (doseq [record# records#]
             (~ingest-name builder# record#))
           (~freeze-name builder#)))

       (defn ~row-id-name [~ds-sym row-key#]
         (sparse-layout.core/id-of (.-rowToId ~ds-sym) row-key#))

       (defn ~col-id-name [~ds-sym col-key#]
         (sparse-layout.core/id-of (.-colToId ~ds-sym) col-key#))

       (defn ~row-name [~ds-sym row-key#]
         (let [row-id# (sparse-layout.core/id-of (.-rowToId ~ds-sym) row-key#)]
           (sparse-layout.core/row-blocks-by-id row-id#
                                                (.-idToCol ~ds-sym)
                                                (.-csrColIds ~ds-sym)
                                                (.-csrRowPtrs ~ds-sym)
                                                (.-payloadKind ~ds-sym)
                                                (.-payloadDim ~ds-sym)
                                                (.-payloadValues ~ds-sym)
                                                (.-payloadPtrs ~ds-sym))))

       (defn ~col-name [~ds-sym col-key#]
         (let [col-id# (sparse-layout.core/id-of (.-colToId ~ds-sym) col-key#)]
           (sparse-layout.core/col-blocks-by-id col-id#
                                                (.-idToRow ~ds-sym)
                                                (.-cscRowIds ~ds-sym)
                                                (.-cscColPtrs ~ds-sym)
                                                (.-cscPayloadIds ~ds-sym)
                                                (.-payloadKind ~ds-sym)
                                                (.-payloadDim ~ds-sym)
                                                (.-payloadValues ~ds-sym)
                                                (.-payloadPtrs ~ds-sym))))

       (defn ~row-views-name [~ds-sym row-key#]
         (let [row-id# (sparse-layout.core/id-of (.-rowToId ~ds-sym) row-key#)]
           (sparse-layout.core/row-block-views-by-id row-id#
                                                     (.-idToCol ~ds-sym)
                                                     (.-csrColIds ~ds-sym)
                                                     (.-csrRowPtrs ~ds-sym)
                                                     (.-payloadKind ~ds-sym)
                                                     (.-payloadDim ~ds-sym)
                                                     (.-payloadValues ~ds-sym)
                                                     (.-payloadPtrs ~ds-sym))))

       (defn ~col-views-name [~ds-sym col-key#]
         (let [col-id# (sparse-layout.core/id-of (.-colToId ~ds-sym) col-key#)]
           (sparse-layout.core/col-block-views-by-id col-id#
                                                     (.-idToRow ~ds-sym)
                                                     (.-cscRowIds ~ds-sym)
                                                     (.-cscColPtrs ~ds-sym)
                                                     (.-cscPayloadIds ~ds-sym)
                                                     (.-payloadKind ~ds-sym)
                                                     (.-payloadDim ~ds-sym)
                                                     (.-payloadValues ~ds-sym)
                                                     (.-payloadPtrs ~ds-sym))))

       (defn ~block-name [~ds-sym row-key# col-key#]
         (let [row-id# (sparse-layout.core/id-of (.-rowToId ~ds-sym) row-key#)
               col-id# (sparse-layout.core/id-of (.-colToId ~ds-sym) col-key#)]
           (sparse-layout.core/block-by-ids row-id#
                                            col-id#
                                            (.-csrRowPtrs ~ds-sym)
                                            (.-csrColIds ~ds-sym)
                                            (.-cscColPtrs ~ds-sym)
                                            (.-cscRowIds ~ds-sym)
                                            (.-cscPayloadIds ~ds-sym)
                                            (.-payloadKind ~ds-sym)
                                            (.-payloadDim ~ds-sym)
                                            (.-payloadValues ~ds-sym)
                                            (.-payloadPtrs ~ds-sym))))

       (defn ~block-view-name [~ds-sym row-key# col-key#]
         (let [row-id# (sparse-layout.core/id-of (.-rowToId ~ds-sym) row-key#)
               col-id# (sparse-layout.core/id-of (.-colToId ~ds-sym) col-key#)]
           (sparse-layout.core/block-view-by-ids row-id#
                                                 col-id#
                                                 (.-csrRowPtrs ~ds-sym)
                                                 (.-csrColIds ~ds-sym)
                                                 (.-cscColPtrs ~ds-sym)
                                                 (.-cscRowIds ~ds-sym)
                                                 (.-cscPayloadIds ~ds-sym)
                                                 (.-payloadKind ~ds-sym)
                                                 (.-payloadDim ~ds-sym)
                                                 (.-payloadValues ~ds-sym)
                                                 (.-payloadPtrs ~ds-sym)))))))
