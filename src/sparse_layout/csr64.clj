(ns sparse-layout.csr64
  "Java 25 MemorySegment-backed CSR storage with 64-bit entry offsets."
  (:require [sparse-layout.csr-source.protocols :as p])
  (:import [java.io Closeable EOFException Writer]
           [java.lang.foreign Arena MemorySegment ValueLayout ValueLayout$OfDouble ValueLayout$OfInt
            ValueLayout$OfLong]
           [java.nio ByteBuffer ByteOrder]
           [java.nio.channels FileChannel FileChannel$MapMode]
           [java.nio.charset StandardCharsets]
           [java.nio.file Files Path Paths StandardOpenOption]
           [java.util Arrays]))

(set! *warn-on-reflection* true)

(def ^:private artifact-version 2)
(def ^:private endian-marker 0x01020304)
(def ^:private header-size 80)
(def ^:private artifact-magic (.getBytes "SLCSR64\u0000" StandardCharsets/US_ASCII))

(def ^:private ^ValueLayout$OfLong le-long
  (.withOrder ValueLayout/JAVA_LONG_UNALIGNED ByteOrder/LITTLE_ENDIAN))
(def ^:private ^ValueLayout$OfInt le-int
  (.withOrder ValueLayout/JAVA_INT_UNALIGNED ByteOrder/LITTLE_ENDIAN))
(def ^:private ^ValueLayout$OfDouble le-double
  (.withOrder ValueLayout/JAVA_DOUBLE_UNALIGNED ByteOrder/LITTLE_ENDIAN))

(defn- path-of
  ^Path [path]
  (cond (instance? Path path) path
        (string? path) (Paths/get ^String path (make-array String 0))
        :else (throw (ex-info "CSR64 path must be a string or Path." {:path path}))))

(defn- checked-add ^long [^long a ^long b] (Math/addExact a b))

(defn- checked-mul ^long [^long a ^long b] (Math/multiplyExact a b))

(defn- align8
  ^long [^long n]
  (let [remainder (mod n 8)]
    (if (zero? remainder) n (checked-add n (- 8 remainder)))))

(defn- artifact-plan
  [{:keys [row-count col-count entry-count block-dim]}]
  (let [row-count
        (long row-count)

        col-count
        (long col-count)

        entry-count
        (long entry-count)

        block-dim
        (long block-dim)]

    (when (or (neg? row-count)
              (neg? col-count)
              (neg? entry-count)
              (not (pos? block-dim))
              (> block-dim Integer/MAX_VALUE)
              (> row-count Integer/MAX_VALUE)
              (> col-count Integer/MAX_VALUE))
      (throw (ex-info "Invalid CSR64 dimensions."
                      {:row-count row-count
                       :col-count col-count
                       :entry-count entry-count
                       :block-dim block-dim})))
    (let [row-ptrs-offset
          (long header-size)

          row-ptrs-bytes
          (checked-mul 8 (inc row-count))

          col-ids-offset
          (align8 (checked-add row-ptrs-offset row-ptrs-bytes))

          col-ids-bytes
          (checked-mul 4 entry-count)

          payload-offset
          (align8 (checked-add col-ids-offset col-ids-bytes))

          payload-elements
          (checked-mul entry-count block-dim)

          payload-bytes
          (checked-mul 8 payload-elements)

          file-size
          (checked-add payload-offset payload-bytes)]

      {:row-count row-count
       :col-count col-count
       :entry-count entry-count
       :block-dim block-dim
       :row-ptrs-offset row-ptrs-offset
       :row-ptrs-bytes row-ptrs-bytes
       :col-ids-offset col-ids-offset
       :col-ids-bytes col-ids-bytes
       :payload-offset payload-offset
       :payload-elements payload-elements
       :payload-bytes payload-bytes
       :file-size file-size})))

(defn- header-buffer
  ^ByteBuffer
  [{:keys [row-count col-count entry-count block-dim row-ptrs-offset col-ids-offset payload-offset
           file-size]}]
  (doto (ByteBuffer/allocate header-size)
    (.order ByteOrder/LITTLE_ENDIAN)
    (.put ^bytes artifact-magic)
    (.putInt artifact-version)
    (.putInt endian-marker)
    (.putInt header-size)
    (.putInt (int block-dim))
    (.putLong row-count)
    (.putLong col-count)
    (.putLong entry-count)
    (.putLong row-ptrs-offset)
    (.putLong col-ids-offset)
    (.putLong payload-offset)
    (.putLong file-size)
    (.flip)))

(defn- write-all!
  [^FileChannel channel ^ByteBuffer buffer ^long position]
  (loop [position position]
    (when (.hasRemaining buffer)
      (let [written (.write channel buffer position)]
        (when-not (pos? written)
          (throw (EOFException. "Could not make progress writing CSR64 artifact.")))
        (recur (+ position written))))))

(defn- read-all!
  [^FileChannel channel ^ByteBuffer buffer ^long position]
  (loop [position position]
    (when (.hasRemaining buffer)
      (let [read (.read channel buffer position)]
        (when (neg? read) (throw (EOFException. "CSR64 artifact ended inside its header.")))
        (when (zero? read)
          (throw (EOFException. "Could not make progress reading CSR64 artifact.")))
        (recur (+ position read))))))

(defn- read-header
  [^FileChannel channel]
  (let [buffer (doto (ByteBuffer/allocate header-size) (.order ByteOrder/LITTLE_ENDIAN))]
    (read-all! channel buffer 0)
    (.flip buffer)
    (let [magic (byte-array (alength ^bytes artifact-magic))]
      (.get buffer magic)
      (when-not (Arrays/equals ^bytes artifact-magic magic)
        (throw (ex-info "CSR64 artifact magic mismatch." {}))))
    (let [version (.getInt buffer)
          endian (.getInt buffer)
          actual-header-size (.getInt buffer)
          block-dim (.getInt buffer)
          row-count (.getLong buffer)
          col-count (.getLong buffer)
          entry-count (.getLong buffer)
          row-ptrs-offset (.getLong buffer)
          col-ids-offset (.getLong buffer)
          payload-offset (.getLong buffer)
          file-size (.getLong buffer)
          header {:row-count row-count
                  :col-count col-count
                  :entry-count entry-count
                  :block-dim block-dim
                  :row-ptrs-offset row-ptrs-offset
                  :col-ids-offset col-ids-offset
                  :payload-offset payload-offset
                  :file-size file-size}]

      (when-not (= artifact-version version)
        (throw (ex-info "Unsupported CSR64 artifact version."
                        {:expected artifact-version :actual version})))
      (when-not (= endian-marker endian)
        (throw (ex-info "CSR64 artifact endian marker mismatch." {:actual endian})))
      (when-not (= header-size actual-header-size)
        (throw (ex-info "CSR64 artifact header size mismatch."
                        {:expected header-size :actual actual-header-size})))
      (let [expected (artifact-plan header)]
        (when-not (= expected (merge expected header))
          (throw (ex-info "CSR64 artifact section offsets are invalid."
                          {:expected (select-keys expected
                                                  [:row-ptrs-offset :col-ids-offset :payload-offset
                                                   :file-size])
                           :actual (select-keys header
                                                [:row-ptrs-offset :col-ids-offset :payload-offset
                                                 :file-size])})))
        expected))))

(defn- extend-file!
  [^FileChannel channel ^long file-size]
  (when (pos? file-size)
    (let [last-byte (ByteBuffer/wrap (byte-array 1))]
      (write-all! channel last-byte (dec file-size)))))

(defn- map-region
  ^MemorySegment [^FileChannel channel mode offset byte-size ^Arena arena]
  (let [offset
        (long offset)

        byte-size
        (long byte-size)]

    (if (zero? byte-size)
      (MemorySegment/ofArray (byte-array 0))
      (.map channel mode offset byte-size arena))))

(defn- row-ptr
  ^long [^MemorySegment row-ptrs ^long row-id]
  (.getAtIndex row-ptrs ValueLayout/JAVA_LONG_UNALIGNED row-id))

(defn- entry-col
  ^long [^MemorySegment col-ids ^long entry-id]
  (.getAtIndex col-ids ValueLayout/JAVA_INT_UNALIGNED entry-id))

(defn- ensure-index!
  ^long [label ^long value ^long upper-bound]
  (when (or (neg? value) (>= value upper-bound))
    (throw (ex-info "CSR64 index is out of bounds."
                    {:index label :value value :upper-bound upper-bound})))
  value)

(defn- ensure-row-range!
  [row-start row-end row-count]
  (let [row-start
        (long row-start)

        row-end
        (long row-end)

        row-count
        (long row-count)]

    (when (or (neg? row-start) (> row-start row-end) (> row-end row-count))
      (throw (ex-info "CSR64 row range is out of bounds."
                      {:row-start row-start :row-end row-end :row-count row-count}))))
  nil)

(defn- range-start
  ^long [r]
  (cond (map? r) (long (:row-start r))
        (vector? r) (long (nth r 0))
        (seq? r) (long (first r))
        :else (throw (ex-info "Unsupported CSR64 row range." {:range r}))))

(defn- range-end
  ^long [r]
  (cond (map? r) (long (:row-end r))
        (vector? r) (long (nth r 1))
        (seq? r) (long (second r))
        :else (throw (ex-info "Unsupported CSR64 row range." {:range r}))))

(defn- page-row-start
  ^long [^MemorySegment row-ptrs ^long row-start ^long row-end ^long entry-start]
  (loop [low
         row-start

         high
         row-end]

    (if (< low high)
      (let [middle (quot (+ low high) 2)]
        (if (<= (row-ptr row-ptrs (inc middle)) entry-start)
          (recur (inc middle) high)
          (recur low middle)))
      low)))

(defn- ensure-copy-target!
  [^ints dst-row-ids ^ints dst-col-ids ^doubles dst-values dst-entry-off entry-count block-dim]
  (let [dst-entry-off
        (long dst-entry-off)

        entry-count
        (long entry-count)

        block-dim
        (long block-dim)

        entry-end
        (checked-add dst-entry-off entry-count)

        value-end
        (checked-mul entry-end block-dim)]

    (when (or (neg? dst-entry-off)
              (> entry-end (alength dst-row-ids))
              (> entry-end (alength dst-col-ids))
              (> value-end (alength dst-values)))
      (throw (ex-info "CSR64 page destination is out of bounds."
                      {:dst-entry-off dst-entry-off
                       :entry-count entry-count
                       :block-dim block-dim
                       :row-id-capacity (alength dst-row-ids)
                       :col-id-capacity (alength dst-col-ids)
                       :value-capacity (alength dst-values)}))))
  nil)

(defn- copy-page-fields!
  [^MemorySegment row-ptrs ^MemorySegment col-ids ^MemorySegment payload row-count block-dim
   row-start row-end cursor max-entries ^ints dst-row-ids ^ints dst-col-ids ^doubles dst-values
   dst-entry-off]
  (let [row-count
        (long row-count)

        block-dim
        (long block-dim)

        row-start
        (long row-start)

        row-end
        (long row-end)

        cursor
        (long cursor)

        max-entries
        (long max-entries)

        dst-entry-off
        (long dst-entry-off)]

    (ensure-row-range! row-start row-end row-count)
    (when (neg? cursor)
      (throw (ex-info "CSR64 page cursor must be non-negative." {:cursor cursor})))
    (when-not (pos? max-entries)
      (throw (ex-info "CSR64 page size must be positive." {:max-entries max-entries})))
    (let [range-entry-start
          (row-ptr row-ptrs row-start)

          range-entry-end
          (row-ptr row-ptrs row-end)

          range-count
          (- range-entry-end range-entry-start)]

      (when (> cursor range-count)
        (throw (ex-info "CSR64 page cursor exceeds the selected range."
                        {:cursor cursor :entry-count range-count})))
      (let [entry-start
            (+ range-entry-start cursor)

            entry-count
            (min max-entries (- range-entry-end entry-start))

            entry-end
            (+ entry-start entry-count)

            value-count
            (checked-mul entry-count block-dim)]

        (when (or (> entry-count Integer/MAX_VALUE) (> value-count Integer/MAX_VALUE))
          (throw (ex-info
                   "CSR64 page exceeds one Java destination array operation."
                   {:entry-count entry-count :value-count value-count :max Integer/MAX_VALUE})))
        (ensure-copy-target! dst-row-ids dst-col-ids dst-values dst-entry-off entry-count block-dim)
        (when (pos? entry-count)
          (MemorySegment/copy col-ids
                              le-int
                              (checked-mul entry-start 4)
                              dst-col-ids
                              (int dst-entry-off)
                              (int entry-count))
          (MemorySegment/copy payload
                              le-double
                              (checked-mul (checked-mul entry-start block-dim) 8)
                              dst-values
                              (int (checked-mul dst-entry-off block-dim))
                              (int value-count))
          (loop [row-id (page-row-start row-ptrs row-start row-end entry-start)]
            (when (and (< row-id row-end) (< (row-ptr row-ptrs row-id) entry-end))
              (let [fill-start (max entry-start (row-ptr row-ptrs row-id))
                    fill-end (min entry-end (row-ptr row-ptrs (inc row-id)))]

                (when (< fill-start fill-end)
                  (Arrays/fill dst-row-ids
                               (int (+ dst-entry-off (- fill-start entry-start)))
                               (int (+ dst-entry-off (- fill-end entry-start)))
                               (int row-id)))
                (recur (inc row-id))))))
        entry-count))))

(defn- copy-block-fields!
  [^MemorySegment payload entry-count block-dim entry-id ^doubles dst dst-off]
  (let [entry-count
        (long entry-count)

        block-dim
        (long block-dim)

        entry-id
        (ensure-index! :entry-id entry-id entry-count)

        dst-off
        (long dst-off)]

    (when (or (neg? dst-off) (> (+ dst-off block-dim) (alength dst)))
      (throw (ex-info "CSR64 block destination is out of bounds."
                      {:dst-off dst-off :block-dim block-dim :dst-length (alength dst)})))
    (MemorySegment/copy payload
                        le-double
                        (checked-mul (checked-mul entry-id block-dim) 8)
                        dst
                        (int dst-off)
                        (int block-dim))
    dst))

(defn- numeric-id
  [value upper-bound]
  (if (and (integer? value) (<= 0 (long value)) (< (long value) (long upper-bound)))
    (long value)
    -1))

(defn- selection-ranges
  [selection row-count]
  (cond (nil? selection) [[0 row-count]]
        (:ranges selection) (:ranges selection)
        (:range selection) [(:range selection)]
        :else (throw (ex-info "CSR64 supports numeric row ranges only." {:selection selection}))))

(deftype CSR64Source [path ^Arena arena ^MemorySegment rowPtrs ^MemorySegment colIds
                      ^MemorySegment payload ^long rowCount ^long colCount ^long entryCount
                      ^long blockDim plan]
  p/CSRSource
    (csr-row-count [_] rowCount)
    (csr-col-count [_] colCount)
    (csr-entry-count [_] entryCount)
    (csr-block-dim [_] blockDim)
    (csr-row-id [_ row-key] (numeric-id row-key rowCount))
    (csr-col-id [_ col-key] (numeric-id col-key colCount))
    (csr-row-key-at [_ row-id] (ensure-index! :row-id row-id rowCount))
    (csr-col-key-at [_ col-id] (ensure-index! :col-id col-id colCount))
    (csr-row-span [_ row-id]
      (let [row-id (ensure-index! :row-id row-id rowCount)]
        [(row-ptr rowPtrs row-id) (row-ptr rowPtrs (inc row-id))]))
    (csr-entry-col-id [_ entry-id] (entry-col colIds (ensure-index! :entry-id entry-id entryCount)))
    (csr-copy-block! [_ entry-id dst dst-off]
      (copy-block-fields! payload entryCount blockDim entry-id dst dst-off))
    (csr-copy-ranges! [_ ranges dst-row-ids dst-col-ids dst-values dst-entry-off]
      (let [total (reduce (fn [sum r]
                            (let [start (range-start r)
                                  end (range-end r)]

                              (ensure-row-range! start end rowCount)
                              (+ sum (- (row-ptr rowPtrs end) (row-ptr rowPtrs start)))))
                          0
                          ranges)]
        (ensure-copy-target! dst-row-ids dst-col-ids dst-values dst-entry-off total blockDim)
        (loop [remaining (seq ranges)
               copied (long 0)]

          (if-let [r (first remaining)]
            (let [start (range-start r)
                  end (range-end r)
                  range-count (- (row-ptr rowPtrs end) (row-ptr rowPtrs start))]

              (copy-page-fields! rowPtrs
                                 colIds
                                 payload
                                 rowCount
                                 blockDim
                                 start
                                 end
                                 0
                                 (max 1 range-count)
                                 dst-row-ids
                                 dst-col-ids
                                 dst-values
                                 (+ (long dst-entry-off) copied))
              (recur (next remaining) (+ copied range-count)))
            copied))))
    (csr-scan-row! [this row-id visitor]
      (let [row-id
            (ensure-index! :row-id row-id rowCount)

            start
            (row-ptr rowPtrs row-id)

            end
            (row-ptr rowPtrs (inc row-id))]

        (loop [entry-id start]
          (when (< entry-id end)
            (visitor row-id (entry-col colIds entry-id) entry-id this)
            (recur (inc entry-id)))))
      nil)
    (csr-resolve-ranges [_ selection] (selection-ranges selection rowCount))
    (csr-scan-ranges! [this ranges visitor]
      (doseq [r ranges]
        (let [start (range-start r)
              end (range-end r)]

          (ensure-row-range! start end rowCount)
          (loop [row-id start]
            (when (< row-id end) (p/csr-scan-row! this row-id visitor) (recur (inc row-id))))))
      nil)
  Closeable
    (close [_] (when (.isAlive (.scope rowPtrs)) (.close arena))))

(definterface PageFill (^void setCount [^long n]))

(deftype Page [^ints rowIds ^ints colIds ^doubles values ^long capacity ^long blockDim
               ^:unsynchronized-mutable ^long filled]
  clojure.lang.Counted
    (count [_] (int filled))
  PageFill
    (setCount [_ n] (set! filled n)))

(defn- print-shape
  [value shape ^Writer writer]
  (.write writer (str "#object[" (.getName (class value)) " "))
  (print-method shape writer)
  (.write writer "]"))

(defmethod print-method CSR64Source
  [^CSR64Source source writer]
  (print-shape source
               {:path (str (.-path source))
                :row-count (.-rowCount source)
                :col-count (.-colCount source)
                :entry-count (.-entryCount source)
                :block-dim (.-blockDim source)
                :open? (.isAlive (.scope ^MemorySegment (.-rowPtrs source)))}
               writer))

(defmethod print-method Page
  [^Page page writer]
  (print-shape page
               {:count (count page) :capacity (.-capacity page) :block-dim (.-blockDim page)}
               writer))

(defn metadata
  "Returns immutable CSR64 artifact metadata without exposing mapped storage."
  [^CSR64Source source]
  (assoc (.-plan source) :path (.-path source)))

(defn row-count
  "Returns the number of rows in the source."
  ^long [^CSR64Source source]
  (.-rowCount source))

(defn col-count
  "Returns the number of columns in the source."
  ^long [^CSR64Source source]
  (.-colCount source))

(defn entry-count
  "Returns the number of stored entries in the source."
  ^long [^CSR64Source source]
  (.-entryCount source))

(defn block-dim
  "Returns the number of doubles in each entry's payload block."
  ^long [^CSR64Source source]
  (.-blockDim source))

(defn range-entry-count
  "Returns the number of entries in the half-open numeric row range."
  ^long [^CSR64Source source row-start row-end]
  (ensure-row-range! row-start row-end (.-rowCount source))
  (- (row-ptr (.-rowPtrs source) (long row-end)) (row-ptr (.-rowPtrs source) (long row-start))))

(defn copy-page!
  "Copies at most `max-entries` from a contiguous row range.

  `cursor` is the number of entries already consumed from the selected range.
  Returns the copied entry count; the next cursor is `cursor + copied`. An empty
  range or an exhausted cursor returns 0 without writing."
  [^CSR64Source source row-start row-end cursor max-entries ^ints dst-row-ids ^ints dst-col-ids
   ^doubles dst-values dst-entry-off]
  (copy-page-fields! (.-rowPtrs source)
                     (.-colIds source)
                     (.-payload source)
                     (.-rowCount source)
                     (.-blockDim source)
                     row-start
                     row-end
                     cursor
                     max-entries
                     dst-row-ids
                     dst-col-ids
                     dst-values
                     dst-entry-off))

(defn page
  "Returns a reusable page buffer for `source`.

  It holds `int[capacity]` row ids, `int[capacity]` column ids and
  `double[capacity × block-dim]` values, which `pages` refills on every step. Throws
  `ex-info` unless `capacity` is positive and `capacity × block-dim` fits in
  `Integer/MAX_VALUE`."
  [^CSR64Source source capacity]
  (let [capacity
        (long capacity)

        dim
        (.-blockDim source)]

    (when (or (< capacity 1) (> capacity (quot Integer/MAX_VALUE (max 1 dim))))
      (throw (ex-info "CSR64 page capacity is out of bounds."
                      {:capacity capacity :block-dim dim :max-value-count Integer/MAX_VALUE})))
    (Page. (int-array capacity)
           (int-array capacity)
           (double-array (* capacity dim))
           capacity
           dim
           0)))

(defn pages
  "Returns a reducible over the entries of a half-open row range, all rows by default.

  Every step receives `buf`, a buffer from `page`, refilled with the next entries,
  so a page is valid only inside the step that receives it. Implements `IReduceInit`
  only: `seq` throws instead of keeping the borrowed buffer alive. Returning `reduced`
  stops the drain before the next copy. Throws `ex-info` for an invalid row range, or
  for a buffer whose block dim differs from the source's."
  ([^CSR64Source source buf] (pages source 0 (.-rowCount source) buf))
  ([^CSR64Source source row-start row-end buf]
   (ensure-row-range! row-start row-end (.-rowCount source))
   (when-not (and (instance? Page buf) (= (.-blockDim ^Page buf) (.-blockDim source)))
     (throw (ex-info "CSR64 page buffer does not match the source's block dim."
                     {:block-dim (.-blockDim source)
                      :buffer-block-dim (when (instance? Page buf) (.-blockDim ^Page buf))})))
   ;; copy-page-fields! takes Object arguments: box the drain's invariants once here
   ;; instead of once per page.
   (let [row-start
         (num (long row-start))

         row-end
         (num (long row-end))

         rows
         (num (.-rowCount source))

         dim
         (num (.-blockDim source))

         capacity
         (num (.-capacity ^Page buf))

         ^Page buf
         buf]

     (reify
       clojure.lang.IReduceInit
         (reduce [_ f init]
           (let [total (range-entry-count source row-start row-end)]
             (loop [cursor 0
                    acc init]

               (if (< cursor total)
                 (let [copied (long (copy-page-fields! (.-rowPtrs source)
                                                       (.-colIds source)
                                                       (.-payload source)
                                                       rows
                                                       dim
                                                       row-start
                                                       row-end
                                                       cursor
                                                       capacity
                                                       (.-rowIds buf)
                                                       (.-colIds buf)
                                                       (.-values buf)
                                                       0))]
                   (.setCount buf copied)
                   (let [acc (f acc buf)]
                     (if (reduced? acc) @acc (recur (+ cursor copied) acc))))
                 acc))))))))

(defn page-row
  "Returns the row id of entry `i` in the current step's page."
  ^long [^Page page ^long i]
  (aget ^ints (.-rowIds page) (ensure-index! :page-entry i (count page))))

(defn page-col
  "Returns the column id of entry `i` in the current step's page."
  ^long [^Page page ^long i]
  (aget ^ints (.-colIds page) (ensure-index! :page-entry i (count page))))

(defn page-lane
  "Returns lane `k` of entry `i`'s block in the current step's page."
  ^double [^Page page ^long i ^long k]
  (let [dim (.-blockDim page)]
    (aget ^doubles (.-values page)
          (+ (* (ensure-index! :page-entry i (count page)) dim) (ensure-index! :lane k dim)))))

(defn page-rows
  "Returns the page's row-id array; only the first `(count page)` ids are valid."
  ^ints [^Page page]
  (.-rowIds page))

(defn page-cols
  "Returns the page's column-id array; only the first `(count page)` ids are valid."
  ^ints [^Page page]
  (.-colIds page))

(defn page-values
  "Returns the page's value array; only the first `(count page)` blocks are valid."
  ^doubles [^Page page]
  (.-values page))

(defn lane
  "Returns lane `k` of entry `entry-id`'s block, read from the mapping.

  A direct three-argument call expands at compile time into the same checked read, so it
  does not go through the var; `apply` and higher-order use call this function."
  {:inline (fn [source entry-id k]
             `(let [^CSR64Source source#
                    ~source

                    entry-id#
                    (long ~entry-id)

                    k#
                    (long ~k)

                    entry-count#
                    (.-entryCount source#)

                    dim#
                    (.-blockDim source#)]

                (when (or (neg? entry-id#) (>= entry-id# entry-count#))
                  (throw (ex-info "CSR64 index is out of bounds."
                                  {:index :entry-id :value entry-id# :upper-bound entry-count#})))
                (when (or (neg? k#) (>= k# dim#))
                  (throw (ex-info "CSR64 index is out of bounds."
                                  {:index :lane :value k# :upper-bound dim#})))
                (.getAtIndex ^MemorySegment (.-payload source#)
                             ValueLayout/JAVA_DOUBLE_UNALIGNED
                             (+ (* entry-id# dim#) k#))))
   :inline-arities #{3}}
  ^double [^CSR64Source source ^long entry-id ^long k]
  (let [dim (.-blockDim source)]
    (.getAtIndex ^MemorySegment (.-payload source)
                 ValueLayout/JAVA_DOUBLE_UNALIGNED
                 (+ (* (ensure-index! :entry-id entry-id (.-entryCount source)) dim)
                    (ensure-index! :lane k dim)))))

(defn- ensure-reduce-entries-form!
  [entry-bindings range-bindings acc-bindings]
  (when-not (and (vector? entry-bindings)
                 (= 3 (count entry-bindings))
                 (every? simple-symbol? entry-bindings)
                 (vector? range-bindings)
                 (contains? #{1 3} (count range-bindings))
                 (vector? acc-bindings)
                 (= 2 (count acc-bindings))
                 (simple-symbol? (first acc-bindings)))
    (throw (ex-info
             "reduce-entries expects [row col e], [src] or [src row-start row-end], and [acc init]."
             {:entry-bindings entry-bindings
              :range-bindings range-bindings
              :acc-bindings acc-bindings}))))

(defmacro reduce-entries
  "Reduces over the entries of a half-open numeric row range, reading the mapping directly.

  `(reduce-entries [row col e] [src row-start row-end] [acc init] body)` binds `row`, `col` and
  the entry id `e` as primitive longs, in `csr-scan-ranges!` order, and binds `acc` to `init`
  and then to each body value; it returns the last one. `[src]` alone covers every row. Rows
  without entries are skipped.

  `src`, the bounds and `init` are evaluated once, in that order. An invalid row range throws
  `ex-info` before `init` is evaluated. The expansion is one flat loop: a primitive `init` and
  body keep `acc` unboxed, and any other body still works, but boxes. The body sits in
  expression position, where an inner `loop` or `dotimes` compiles into a closure; put
  multi-lane work in a primitive-typed helper and call it from the body. `reduced` does not
  stop the loop. The source must stay open until the call returns.

  Read single lanes with `lane`. To copy whole blocks, call
  `sparse-layout.csr-source/csr-copy-block!` once per entry instead of `lane` once per double:

    (reduce-entries [row col e] [src row-start row-end] [slot 0]
      (aset row-ids slot (int row))
      (aset col-ids slot (int col))
      (csr-copy-block! src e values (* slot block-dim))
      (inc slot))"
  [entry-bindings range-bindings acc-bindings & body]
  (ensure-reduce-entries-form! entry-bindings range-bindings acc-bindings)
  (let [[row col e]
        entry-bindings

        [source row-start row-end]
        range-bindings

        [acc init]
        acc-bindings

        bounded?
        (= 3 (count range-bindings))

        src
        (vary-meta (gensym "src") assoc :tag `CSR64Source)]

    `(let [~src
           ~source

           start-row#
           ~(if bounded? `(long ~row-start) 0)

           end-row#
           ~(if bounded? `(long ~row-end) `(row-count ~src))

           entry-count#
           (range-entry-count ~src start-row# end-row#)

           ^MemorySegment row-ptrs#
           (.-rowPtrs ~src)

           ^MemorySegment col-ids#
           (.-colIds ~src)

           start-entry#
           (.getAtIndex row-ptrs# ValueLayout/JAVA_LONG_UNALIGNED start-row#)

           end-entry#
           (+ start-entry# entry-count#)]

       (loop [entry#
              start-entry#

              row#
              (unchecked-dec start-row#)

              row-end-entry#
              start-entry#

              acc#
              ~init]

         (if (< entry# end-entry#)
           (if (< entry# row-end-entry#)
             (let [~row
                   row#

                   ~col
                   (long (.getAtIndex col-ids# ValueLayout/JAVA_INT_UNALIGNED entry#))

                   ~e
                   entry#

                   ~acc
                   acc#]

               (recur (unchecked-inc entry#) row# row-end-entry# (do ~@body)))
             (let [next-row# (unchecked-inc row#)]
               (recur
                 entry#
                 next-row#
                 (.getAtIndex row-ptrs# ValueLayout/JAVA_LONG_UNALIGNED (unchecked-inc next-row#))
                 acc#)))
           acc#)))))

(defn find-entry
  "Returns the entry id for a numeric row/column coordinate, or -1."
  ^long [^CSR64Source source ^long row-id ^long col-id]
  (let [row-count
        (.-rowCount source)

        col-count
        (.-colCount source)

        ^MemorySegment row-ptrs
        (.-rowPtrs source)

        ^MemorySegment col-ids
        (.-colIds source)]

    (when (or (neg? row-id) (>= row-id row-count) (neg? col-id) (>= col-id col-count))
      (ensure-index! :row-id row-id row-count)
      (ensure-index! :col-id col-id col-count))
    (loop [low
           (.getAtIndex row-ptrs ValueLayout/JAVA_LONG_UNALIGNED row-id)

           high
           (dec (.getAtIndex row-ptrs ValueLayout/JAVA_LONG_UNALIGNED (inc row-id)))]

      (if (<= low high)
        (let [middle
              (quot (+ low high) 2)

              actual
              (long (.getAtIndex col-ids ValueLayout/JAVA_INT_UNALIGNED middle))]

          (cond (< actual col-id) (recur (inc middle) high)
                (> actual col-id) (recur low (dec middle))
                :else middle))
        -1))))

(defn copy-point!
  "Copies one numeric coordinate into `dst`; returns `dst`, or nil when absent.

  Without `dst-off`, copies to offset 0. That arity takes primitive long ids, so a direct call
  with long ids does not box them."
  ([^CSR64Source source ^long row-id ^long col-id ^doubles dst]
   (let [entry-id (find-entry source row-id col-id)]
     (when-not (= -1 entry-id)
       (copy-block-fields! (.-payload source)
                           (.-entryCount source)
                           (.-blockDim source)
                           entry-id
                           dst
                           0))))
  ([^CSR64Source source row-id col-id ^doubles dst dst-off]
   (let [entry-id (find-entry source row-id col-id)]
     (when-not (= -1 entry-id)
       (copy-block-fields! (.-payload source)
                           (.-entryCount source)
                           (.-blockDim source)
                           entry-id
                           dst
                           dst-off)))))

(defn load!
  "Best-effort preload of every mapped CSR64 section."
  [^CSR64Source source]
  (.load ^MemorySegment (.-rowPtrs source))
  (when (pos? (.-entryCount source))
    (.load ^MemorySegment (.-colIds source))
    (.load ^MemorySegment (.-payload source)))
  source)

(defn loaded?
  "Returns the operating system's point-in-time residency hint."
  [^CSR64Source source]
  (and (.isLoaded ^MemorySegment (.-rowPtrs source))
       (or (zero? (.-entryCount source))
           (and (.isLoaded ^MemorySegment (.-colIds source))
                (.isLoaded ^MemorySegment (.-payload source))))))

(defn- validate-mapped-csr!
  [^MemorySegment row-ptrs ^MemorySegment col-ids {:keys [row-count col-count entry-count]}]
  (when-not (zero? (row-ptr row-ptrs 0))
    (throw (ex-info "CSR64 row pointers must start at zero." {})))
  (loop [row-id
         (long 0)

         previous
         (long 0)]

    (when (<= row-id row-count)
      (let [actual (row-ptr row-ptrs row-id)]
        (when (or (< actual previous) (> actual entry-count))
          (throw (ex-info "CSR64 row pointers are invalid."
                          {:row-id row-id :previous previous :actual actual})))
        (recur (inc row-id) actual))))
  (when-not (= entry-count (row-ptr row-ptrs row-count))
    (throw (ex-info "CSR64 final row pointer must equal entry count."
                    {:final-row-pointer (row-ptr row-ptrs row-count) :entry-count entry-count})))
  (loop [entry-id (long 0)]
    (when (< entry-id entry-count)
      (let [col-id (entry-col col-ids entry-id)]
        (when (or (neg? col-id) (>= col-id col-count))
          (throw (ex-info "CSR64 column id is out of bounds."
                          {:entry-id entry-id :col-id col-id :col-count col-count})))
        (recur (inc entry-id)))))
  nil)

(defn- ensure-little-endian-host!
  [^ByteOrder native-order]
  (when-not (= ByteOrder/LITTLE_ENDIAN native-order)
    (throw (ex-info "CSR64 reads require a little-endian host."
                    {:native-order (str native-order)})))
  nil)

(defn open-artifact
  "Opens a v2 CSR64 artifact through shared, read-only MemorySegments.

  Reads use the host's native byte order, so big-endian hosts are rejected."
  ^CSR64Source [path]
  (ensure-little-endian-host! (ByteOrder/nativeOrder))
  (let [path
        (path-of path)

        arena
        (Arena/ofShared)]

    (try
      (with-open [channel
                  (FileChannel/open path (into-array StandardOpenOption [StandardOpenOption/READ]))]
        (let [plan (read-header channel)
              actual-size (.size channel)]

          (when-not (= (:file-size plan) actual-size)
            (throw (ex-info "CSR64 artifact file size mismatch."
                            {:expected (:file-size plan) :actual actual-size})))
          (let [row-ptrs (map-region channel
                                     FileChannel$MapMode/READ_ONLY
                                     (:row-ptrs-offset plan)
                                     (:row-ptrs-bytes plan)
                                     arena)
                col-ids (map-region channel
                                    FileChannel$MapMode/READ_ONLY
                                    (:col-ids-offset plan)
                                    (:col-ids-bytes plan)
                                    arena)
                payload (map-region channel
                                    FileChannel$MapMode/READ_ONLY
                                    (:payload-offset plan)
                                    (:payload-bytes plan)
                                    arena)]

            (validate-mapped-csr! row-ptrs col-ids plan)
            (->CSR64Source path
                           arena
                           row-ptrs
                           col-ids
                           payload
                           (:row-count plan)
                           (:col-count plan)
                           (:entry-count plan)
                           (:block-dim plan)
                           plan))))
      (catch Throwable t (.close arena) (throw t)))))

(defn- write-row-ptrs!
  [source ^MemorySegment row-ptrs row-count]
  (.setAtIndex row-ptrs le-long 0 0)
  (dotimes [row-id (int row-count)]
    (let [[_ end] (p/csr-row-span source row-id)]
      (.setAtIndex row-ptrs le-long (inc row-id) (long end)))))

(defn- chunk-row-end
  ^long [source ^long row-start ^long row-count ^long page-entries]
  (let [[entry-start first-end] (p/csr-row-span source row-start)]
    (if (> (- (long first-end) (long entry-start)) page-entries)
      row-start
      (loop [row-end (inc row-start)]
        (if (< row-end row-count)
          (let [[_ candidate-end] (p/csr-row-span source row-end)]
            (if (<= (- (long candidate-end) (long entry-start)) page-entries)
              (recur (inc row-end))
              row-end))
          row-count)))))

(defn- write-entry-by-entry!
  [source ^MemorySegment col-ids ^MemorySegment payload block-dim row-id ^doubles block]
  (let [block-dim
        (long block-dim)

        row-id
        (long row-id)

        [entry-start entry-end]
        (p/csr-row-span source row-id)]

    (loop [entry-id (long entry-start)]
      (when (< entry-id entry-end)
        (.setAtIndex col-ids le-int entry-id (int (p/csr-entry-col-id source entry-id)))
        (p/csr-copy-block! source entry-id block 0)
        (MemorySegment/copy block
                            0
                            payload
                            le-double
                            (checked-mul (checked-mul entry-id block-dim) 8)
                            (int block-dim))
        (recur (inc entry-id))))))

(defn- write-entries!
  [source ^MemorySegment col-ids ^MemorySegment payload {:keys [row-count block-dim]} page-entries]
  (let [page-entries
        (long (max 1 (min (long page-entries) (quot Integer/MAX_VALUE (long block-dim)))))

        rows
        (int-array (int page-entries))

        cols
        (int-array (int page-entries))

        values
        (double-array (int (* page-entries block-dim)))

        block
        (double-array (int block-dim))]

    (loop [row-start (long 0)]
      (when (< row-start row-count)
        (let [row-end (chunk-row-end source row-start row-count page-entries)]
          (if (= row-start row-end)
            (do (write-entry-by-entry! source col-ids payload block-dim row-start block)
                (recur (inc row-start)))
            (let [[entry-start _] (p/csr-row-span source row-start)
                  copied (long (p/csr-copy-ranges! source [[row-start row-end]] rows cols values 0))
                  value-count (int (* copied block-dim))]

              (MemorySegment/copy cols
                                  0
                                  col-ids
                                  le-int
                                  (checked-mul (long entry-start) 4)
                                  (int copied))
              (MemorySegment/copy values
                                  0
                                  payload
                                  le-double
                                  (checked-mul (checked-mul (long entry-start) block-dim) 8)
                                  value-count)
              (recur row-end))))))))

(defn write-artifact!
  "Writes a numeric-key v2 CSR64 artifact from an existing CSRSource.

  The writer uses bounded caller buffers and never materializes the complete
  payload as one Java array."
  ([source path] (write-artifact! source path nil))
  ([source path {:keys [page-entries] :or {page-entries 4096}}]
   (let [path
         (path-of path)

         parent
         (.getParent path)

         plan
         (artifact-plan {:row-count (p/csr-row-count source)
                         :col-count (p/csr-col-count source)
                         :entry-count (p/csr-entry-count source)
                         :block-dim (p/csr-block-dim source)})]

     (when parent
       (Files/createDirectories parent (make-array java.nio.file.attribute.FileAttribute 0)))
     (with-open [channel (FileChannel/open path
                                           (into-array StandardOpenOption
                                                       [StandardOpenOption/CREATE
                                                        StandardOpenOption/READ
                                                        StandardOpenOption/WRITE
                                                        StandardOpenOption/TRUNCATE_EXISTING]))]
       (extend-file! channel (:file-size plan))
       (write-all! channel (header-buffer plan) 0)
       (with-open [arena (Arena/ofConfined)]
         (let [row-ptrs (map-region channel
                                    FileChannel$MapMode/READ_WRITE
                                    (:row-ptrs-offset plan)
                                    (:row-ptrs-bytes plan)
                                    arena)
               col-ids (map-region channel
                                   FileChannel$MapMode/READ_WRITE
                                   (:col-ids-offset plan)
                                   (:col-ids-bytes plan)
                                   arena)
               payload (map-region channel
                                   FileChannel$MapMode/READ_WRITE
                                   (:payload-offset plan)
                                   (:payload-bytes plan)
                                   arena)]

           (write-row-ptrs! source row-ptrs (:row-count plan))
           (write-entries! source col-ids payload plan page-entries)
           (.force row-ptrs)
           (when (pos? (:entry-count plan)) (.force col-ids) (.force payload))))
       (.force channel true))
     path)))
