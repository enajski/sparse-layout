(ns sparse-layout.csr64
  "Java 25 MemorySegment-backed CSR storage with 64-bit entry offsets."
  (:require [sparse-layout.csr-source.protocols :as p])
  (:import [java.io Closeable EOFException]
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

(defn- row-ptr ^long [^MemorySegment row-ptrs ^long row-id] (.getAtIndex row-ptrs le-long row-id))

(defn- entry-col
  ^long [^MemorySegment col-ids ^long entry-id]
  (.getAtIndex col-ids le-int entry-id))

(defn- ensure-index!
  ^long [label value upper-bound]
  (let [value
        (long value)

        upper-bound
        (long upper-bound)]

    (when (or (neg? value) (>= value upper-bound))
      (throw (ex-info "CSR64 index is out of bounds."
                      {:index label :value value :upper-bound upper-bound})))
    value))

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
                (recur (inc row-id)))))
          entry-count)))))

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

(defn metadata
  "Returns immutable CSR64 artifact metadata without exposing mapped storage."
  [^CSR64Source source]
  (assoc (.-plan source) :path (.-path source)))

(defn range-entry-count
  "Returns the number of entries in the half-open numeric row range."
  ^long [^CSR64Source source row-start row-end]
  (ensure-row-range! row-start row-end (.-rowCount source))
  (- (row-ptr (.-rowPtrs source) (long row-end)) (row-ptr (.-rowPtrs source) (long row-start))))

(defn copy-page!
  "Copies at most `max-entries` from a contiguous row range.

  `cursor` is the number of entries already consumed from the selected range.
  Returns the copied entry count; the next cursor is `cursor + copied`."
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

(defn find-entry
  "Returns the entry id for a numeric row/column coordinate, or -1."
  ^long [^CSR64Source source row-id col-id]
  (let [row-id
        (ensure-index! :row-id row-id (.-rowCount source))

        col-id
        (ensure-index! :col-id col-id (.-colCount source))

        row-ptrs
        (.-rowPtrs source)

        col-ids
        (.-colIds source)]

    (loop [low
           (row-ptr row-ptrs row-id)

           high
           (dec (row-ptr row-ptrs (inc row-id)))]

      (if (<= low high)
        (let [middle
              (quot (+ low high) 2)

              actual
              (entry-col col-ids middle)]

          (cond (< actual col-id) (recur (inc middle) high)
                (> actual col-id) (recur low (dec middle))
                :else middle))
        -1))))

(defn copy-point!
  "Copies one numeric coordinate into `dst`; returns true when present."
  [^CSR64Source source row-id col-id ^doubles dst dst-off]
  (let [entry-id (find-entry source row-id col-id)]
    (when-not (= -1 entry-id)
      (copy-block-fields! (.-payload source)
                          (.-entryCount source)
                          (.-blockDim source)
                          entry-id
                          dst
                          dst-off)
      true)))

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

(defn open-artifact
  "Opens a v2 CSR64 artifact through shared, read-only MemorySegments."
  ^CSR64Source [path]
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
