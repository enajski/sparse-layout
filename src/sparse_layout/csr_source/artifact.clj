(ns sparse-layout.csr-source.artifact
  (:require [sparse-layout.core :as sparse]
            [sparse-layout.csr-source.protocols :as p])
  (:import [java.io ByteArrayOutputStream]
           [java.nio ByteBuffer ByteOrder MappedByteBuffer]
           [java.nio.channels FileChannel FileChannel$MapMode]
           [java.nio.charset StandardCharsets]
           [java.nio.file Files Path Paths StandardOpenOption]
           [java.util HashMap Map]))

(def ^:private artifact-version 1)
(def ^:private endian-marker 0x01020304)
(def ^:private header-size 64)
(def ^:private section-entry-size 32)

(def ^:private artifact-magic-prefix "SLCSR")

(def ^:private ^"[B" artifact-magic
  (byte-array
   (concat (seq (.getBytes artifact-magic-prefix StandardCharsets/US_ASCII))
           [(byte 0) (byte artifact-version) (byte 0)])))

(def ^:private section-ids
  {:row-ptrs 1
   :col-ids 2
   :payload-values 3
   :row-key-offsets 4
   :row-key-bytes 5
   :col-key-offsets 6
   :col-key-bytes 7
   :metadata 8})

(def ^:private section-names
  (into {} (map (fn [[k v]] [v k]) section-ids)))

(def ^:private type-ids
  {:int32 1
   :float64 2
   :int64 3
   :bytes 4
   :utf8-json 5})

(def ^:private type-names
  (into {} (map (fn [[k v]] [v k]) type-ids)))

(def ^:private key-tags
  {:nil 0
   :false 1
   :true 2
   :int64 3
   :float64 4
   :string 5
   :keyword 6
   :vector 7})

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

(defn- align8 ^long [^long n]
  (let [rem (mod n 8)]
    (if (zero? rem)
      n
      (+ n (- 8 rem)))))

(defn- checked-index ^long [^long offset]
  (when (or (neg? offset) (> offset Integer/MAX_VALUE))
    (throw (ex-info "Artifact offset is outside the supported Java ByteBuffer range."
                    {:offset offset
                     :max Integer/MAX_VALUE})))
  offset)

(defn- ensure-index! [label value upper-bound]
  (let [value (long value)
        upper-bound (long upper-bound)]
    (when (or (neg? value) (>= value upper-bound))
      (throw (ex-info "CSRSource index is out of bounds."
                      {:index label
                       :value value
                       :upper-bound upper-bound})))
    value))

(defn- ensure-copy-target! [dst dst-off block-dim]
  (let [dst-len (alength ^doubles dst)
        dst-off (long dst-off)
        block-dim (long block-dim)]
    (when (or (neg? dst-off) (> (+ dst-off block-dim) dst-len))
      (throw (ex-info "CSRSource copy destination is out of bounds."
                      {:dst-off dst-off
                       :block-dim block-dim
                       :dst-length dst-len})))
    dst-off))

(defn- checked-byte-length ^long [type count]
  (let [width (case type
                :int32 4
                :float64 8
                :int64 8
                :bytes 1
                :utf8-json 1)]
    (* (long width) (long count))))

(defn- path-of ^Path [path]
  (cond
    (instance? Path path) path
    (string? path) (Paths/get ^String path (make-array String 0))
    :else (throw (ex-info "Artifact path must be a java.nio.file.Path or string."
                          {:path path
                           :class (some-> path class str)}))))

(defn- put-int-le! [^ByteArrayOutputStream out ^long value]
  (.write out (bit-and value 0xff))
  (.write out (bit-and (unsigned-bit-shift-right value 8) 0xff))
  (.write out (bit-and (unsigned-bit-shift-right value 16) 0xff))
  (.write out (bit-and (unsigned-bit-shift-right value 24) 0xff)))

(defn- put-long-le! [^ByteArrayOutputStream out ^long value]
  (dotimes [shift 8]
    (.write out (bit-and (unsigned-bit-shift-right value (* shift 8)) 0xff))))

(defn- put-double-le! [^ByteArrayOutputStream out ^double value]
  (put-long-le! out (Double/doubleToRawLongBits value)))

(defn- utf8-bytes ^bytes [value]
  (.getBytes ^String value "UTF-8"))

(defn- put-bytes! [^ByteArrayOutputStream out ^bytes bytes]
  (.write out bytes 0 (alength bytes)))

(defn- encode-string! [^ByteArrayOutputStream out value]
  (let [bytes (utf8-bytes value)]
    (put-int-le! out (alength bytes))
    (put-bytes! out bytes)))

(defn- encode-key! [^ByteArrayOutputStream out value]
  (cond
    (nil? value)
    (.write out (int (:nil key-tags)))

    (false? value)
    (.write out (int (:false key-tags)))

    (true? value)
    (.write out (int (:true key-tags)))

    (integer? value)
    (do
      (.write out (int (:int64 key-tags)))
      (put-long-le! out (long value)))

    (float? value)
    (do
      (.write out (int (:float64 key-tags)))
      (put-double-le! out (double value)))

    (string? value)
    (do
      (.write out (int (:string key-tags)))
      (encode-string! out value))

    (keyword? value)
    (do
      (.write out (int (:keyword key-tags)))
      (if-let [ns (namespace value)]
        (encode-string! out ns)
        (put-int-le! out -1))
      (encode-string! out (name value)))

    (vector? value)
    (do
      (.write out (int (:vector key-tags)))
      (put-int-le! out (count value))
      (doseq [item value]
        (encode-key! out item)))

    :else
    (throw (ex-info "CSR artifact keys must be nil, booleans, integers, doubles, strings, keywords, or vectors."
                    {:key value
                     :class (some-> value class str)}))))

(defn- encode-key-section [keys]
  (let [offsets (long-array (inc (count keys)))
        out (ByteArrayOutputStream.)]
    (doseq [[idx key] (map-indexed vector keys)]
      (encode-key! out key)
      (aset offsets (inc idx) (.size out)))
    {:offsets offsets
     :bytes (.toByteArray out)}))

(defn- buffer-order! ^ByteBuffer [^ByteBuffer buffer]
  (.order buffer ByteOrder/LITTLE_ENDIAN))

(defn- bb-get-int ^long [^ByteBuffer buffer ^long offset]
  (.getInt buffer (int (checked-index offset))))

(defn- bb-get-long ^long [^ByteBuffer buffer ^long offset]
  (.getLong buffer (int (checked-index offset))))

(defn- bb-get-double ^double [^ByteBuffer buffer ^long offset]
  (.getDouble buffer (int (checked-index offset))))

(defn- read-bytes ^bytes [^ByteBuffer buffer ^long offset ^long length]
  (let [length (int length)
        out (byte-array length)
        dup (.duplicate buffer)]
    (buffer-order! dup)
    (.position dup (int (checked-index offset)))
    (.get dup out)
    out))

(defn- decode-string-at [^ByteBuffer buffer ^long offset ^long limit]
  (let [length (bb-get-int buffer offset)
        next (+ offset 4)]
    (when (neg? length)
      (throw (ex-info "Negative string length in CSR artifact key."
                      {:offset offset
                       :length length})))
    (let [end (+ next length)]
      (when (> end limit)
        (throw (ex-info "String key extends past key boundary."
                        {:offset offset
                         :end end
                         :limit limit})))
      (let [bytes ^bytes (read-bytes buffer next length)]
        [(String. bytes StandardCharsets/UTF_8) end]))))

(defn- decode-key-at [^ByteBuffer buffer ^long offset ^long limit]
  (when (>= offset limit)
    (throw (ex-info "Missing CSR artifact key tag."
                    {:offset offset
                     :limit limit})))
  (let [tag (bit-and (.get buffer (int (checked-index offset))) 0xff)
        next (inc offset)]
    (case tag
      0 [nil next]
      1 [false next]
      2 [true next]
      3 [(bb-get-long buffer next) (+ next 8)]
      4 [(bb-get-double buffer next) (+ next 8)]
      5 (decode-string-at buffer next limit)
      6 (let [ns-length (bb-get-int buffer next)
              after-ns-length (+ next 4)
              [ns-str after-ns] (if (neg? ns-length)
                                  [nil after-ns-length]
                                  (let [end (+ after-ns-length ns-length)]
                                    (when (> end limit)
                                      (throw (ex-info "Keyword namespace extends past key boundary."
                                                      {:offset offset
                                                       :end end
                                                       :limit limit})))
                                    (let [bytes ^bytes (read-bytes buffer after-ns-length ns-length)]
                                      [(String. bytes StandardCharsets/UTF_8) end])))
              [name-str after-name] (decode-string-at buffer after-ns limit)]
          [(if ns-str
             (keyword ns-str name-str)
             (keyword name-str))
           after-name])
      7 (let [n (bb-get-int buffer next)]
          (when (neg? n)
            (throw (ex-info "Negative vector length in CSR artifact key."
                            {:offset offset
                             :length n})))
          (loop [idx 0
                 pos (long (+ next 4))
                 out (transient [])]
            (if (= idx n)
              [(persistent! out) pos]
              (let [[value next-pos] (decode-key-at buffer pos limit)]
                (recur (inc idx) (long next-pos) (conj! out value))))))
      (throw (ex-info "Unsupported CSR artifact key tag."
                      {:tag tag
                       :offset offset})))))

(defn- decode-key-section [^ByteBuffer buffer offsets-section bytes-section expected-count]
  (let [offsets-offset (:offset offsets-section)
        bytes-offset (:offset bytes-section)
        byte-count (:byte-length bytes-section)
        out (object-array expected-count)]
    (dotimes [idx expected-count]
      (let [start (bb-get-long buffer (+ offsets-offset (* idx 8)))
            end (bb-get-long buffer (+ offsets-offset (* (inc idx) 8)))
            absolute-start (+ bytes-offset start)
            absolute-end (+ bytes-offset end)]
        (when (or (neg? start) (< end start) (> end byte-count))
          (throw (ex-info "Invalid CSR artifact key offsets."
                          {:index idx
                           :start start
                           :end end
                           :byte-count byte-count})))
        (let [[value next-pos] (decode-key-at buffer absolute-start absolute-end)]
          (when-not (= next-pos absolute-end)
            (throw (ex-info "CSR artifact key decoder did not consume the full key."
                            {:index idx
                             :next-position next-pos
                             :end absolute-end})))
          (aset out idx value))))
    out))

(defn- id-map ^HashMap [^objects keys]
  (let [m (HashMap.)]
    (dotimes [idx (alength keys)]
      (.put m (aget keys idx) (int idx)))
    m))

(defn- write-all! [^FileChannel channel ^ByteBuffer buffer ^long position]
  (loop [pos position]
    (when (.hasRemaining buffer)
      (let [n (.write channel buffer pos)]
        (when (neg? n)
          (throw (ex-info "Unexpected EOF while writing CSR artifact."
                          {:position pos})))
        (recur (+ pos n))))))

(defn- section [name type count offset byte-length]
  {:id (section-ids name)
   :name name
   :type-id (type-ids type)
   :type type
   :count (long count)
   :offset (long offset)
   :byte-length (long byte-length)})

(defn- required-section [sections name]
  (or (get sections (section-ids name))
      (throw (ex-info "CSR artifact is missing a required section."
                      {:section name}))))

(defn- metadata-json [row-count col-count entry-count block-dim]
  (str "{\"format\":\"sparse-layout-csr\","
       "\"version\":" artifact-version ","
       "\"payload\":\"fixed-double-block\","
       "\"rowCount\":" row-count ","
       "\"columnCount\":" col-count ","
       "\"entryCount\":" entry-count ","
       "\"blockDim\":" block-dim "}"))

(defn- section-plan [source]
  (let [row-count (p/csr-row-count source)
        col-count (p/csr-col-count source)
        entry-count (p/csr-entry-count source)
        block-dim (p/csr-block-dim source)
        row-keys (mapv #(p/csr-row-key-at source %) (range row-count))
        col-keys (mapv #(p/csr-col-key-at source %) (range col-count))
        row-key-section (encode-key-section row-keys)
        col-key-section (encode-key-section col-keys)
        metadata-bytes (utf8-bytes (metadata-json row-count col-count entry-count block-dim))
        raw [{:name :row-ptrs :type :int32 :count (inc row-count) :byte-length (* 4 (inc row-count))}
             {:name :col-ids :type :int32 :count entry-count :byte-length (* 4 entry-count)}
             {:name :payload-values :type :float64 :count (* entry-count block-dim) :byte-length (* 8 entry-count block-dim)}
             {:name :row-key-offsets :type :int64 :count (inc row-count) :byte-length (* 8 (inc row-count))}
             {:name :row-key-bytes :type :bytes :count (alength ^bytes (:bytes row-key-section)) :byte-length (alength ^bytes (:bytes row-key-section))}
             {:name :col-key-offsets :type :int64 :count (inc col-count) :byte-length (* 8 (inc col-count))}
             {:name :col-key-bytes :type :bytes :count (alength ^bytes (:bytes col-key-section)) :byte-length (alength ^bytes (:bytes col-key-section))}
             {:name :metadata :type :utf8-json :count (alength metadata-bytes) :byte-length (alength metadata-bytes)}]
        data-start (align8 (+ header-size (* section-entry-size (count raw))))]
    (loop [sections []
           offset data-start
           remaining raw]
      (if (empty? remaining)
        {:row-count row-count
         :col-count col-count
         :entry-count entry-count
         :block-dim block-dim
         :sections sections
         :row-key-section row-key-section
         :col-key-section col-key-section
         :metadata-bytes metadata-bytes
         :file-size offset}
        (let [{:keys [name type count byte-length]} (first remaining)
              offset (align8 offset)]
          (recur (conj sections (section name type count offset byte-length))
                 (long (+ offset byte-length))
                 (rest remaining)))))))

(defn- header-buffer [{:keys [row-count col-count entry-count block-dim sections]}]
  (let [buffer ^ByteBuffer (buffer-order! (ByteBuffer/allocate header-size))
        magic ^"[B" artifact-magic]
    (.put buffer magic)
    (.putInt buffer artifact-version)
    (.putInt buffer endian-marker)
    (.putInt buffer header-size)
    (.putInt buffer (count sections))
    (.putLong buffer header-size)
    (.putLong buffer (long row-count))
    (.putLong buffer (long col-count))
    (.putLong buffer (long entry-count))
    (.putInt buffer (int block-dim))
    (.putInt buffer 0)
    (.flip buffer)
    buffer))

(defn- section-table-buffer [sections]
  (let [buffer ^ByteBuffer (buffer-order! (ByteBuffer/allocate (* section-entry-size (count sections))))]
    (doseq [{:keys [id type-id count offset byte-length]} sections]
      (.putInt buffer (int id))
      (.putInt buffer (int type-id))
      (.putLong buffer (long count))
      (.putLong buffer (long offset))
      (.putLong buffer (long byte-length)))
    (.flip buffer)
    buffer))

(defn- write-int-section! [^FileChannel channel section values]
  (let [buffer ^ByteBuffer (buffer-order! (ByteBuffer/allocate (int (:byte-length section))))]
    (doseq [value values]
      (.putInt buffer (int value)))
    (.flip buffer)
    (write-all! channel buffer (:offset section))))

(defn- write-long-array-section! [^FileChannel channel section ^longs values]
  (let [buffer ^ByteBuffer (buffer-order! (ByteBuffer/allocate (int (:byte-length section))))]
    (dotimes [idx (alength values)]
      (.putLong buffer (aget values idx)))
    (.flip buffer)
    (write-all! channel buffer (:offset section))))

(defn- write-byte-section! [^FileChannel channel section ^bytes values]
  (write-all! channel (ByteBuffer/wrap values) (:offset section)))

(defn- write-payload-section! [^FileChannel channel source section]
  (let [block-dim (p/csr-block-dim source)
        entry-count (p/csr-entry-count source)
        block (double-array block-dim)
        buffer ^ByteBuffer (buffer-order! (ByteBuffer/allocate (* 8 block-dim)))]
    (dotimes [entry-id entry-count]
      (java.util.Arrays/fill block 0.0)
      (p/csr-copy-block! source entry-id block 0)
      (.clear buffer)
      (dotimes [idx block-dim]
        (.putDouble buffer (aget block idx)))
      (.flip buffer)
      (write-all! channel buffer (+ (:offset section) (* 8 block-dim entry-id))))))

(defn- write-artifact-sections! [^FileChannel channel source plan section-by-name]
  (let [row-count (:row-count plan)
        entry-count (:entry-count plan)]
    (write-int-section! channel
                        (:row-ptrs section-by-name)
                        (concat (map (fn [row-id]
                                       (first (p/csr-row-span source row-id)))
                                     (range row-count))
                                [(if (zero? row-count)
                                   0
                                   (second (p/csr-row-span source (dec row-count))))]))
    (write-int-section! channel
                        (:col-ids section-by-name)
                        (map #(p/csr-entry-col-id source %) (range entry-count)))
    (write-payload-section! channel source (:payload-values section-by-name))
    (write-long-array-section! channel
                               (:row-key-offsets section-by-name)
                               (:offsets (:row-key-section plan)))
    (write-byte-section! channel
                         (:row-key-bytes section-by-name)
                         (:bytes (:row-key-section plan)))
    (write-long-array-section! channel
                               (:col-key-offsets section-by-name)
                               (:offsets (:col-key-section plan)))
    (write-byte-section! channel
                         (:col-key-bytes section-by-name)
                         (:bytes (:col-key-section plan)))
    (write-byte-section! channel
                         (:metadata section-by-name)
                         (:metadata-bytes plan))))

(defn write-csr-artifact!
  "Writes a language-neutral v1 CSR artifact for a fixed-double-block CSRSource.
  The artifact is a single little-endian binary file with primitive CSR
  sections and typed row/column key dictionaries."
  ([source path] (write-csr-artifact! source path nil))
  ([source path _opts]
   (let [block-dim (p/csr-block-dim source)]
     (when-not (pos? block-dim)
       (throw (ex-info "CSR artifacts require a positive fixed block dimension."
                       {:block-dim block-dim})))
     (let [path (path-of path)
           parent (.getParent path)
           plan (section-plan source)
           sections (:sections plan)
           section-by-name (into {} (map (juxt :name identity) sections))]
       (when parent
         (Files/createDirectories parent (make-array java.nio.file.attribute.FileAttribute 0)))
       (with-open [channel (FileChannel/open path
                                             (into-array StandardOpenOption
                                                         [StandardOpenOption/CREATE
                                                          StandardOpenOption/WRITE
                                                          StandardOpenOption/TRUNCATE_EXISTING]))]
         (write-all! channel (header-buffer plan) 0)
         (write-all! channel (section-table-buffer sections) header-size)
         (write-artifact-sections! channel source plan section-by-name)
         (.truncate channel (long (:file-size plan)))
         (.force channel true))
       path))))

(defn- validate-section! [file-size {:keys [id name type-id offset byte-length count]}]
  (when-not (contains? section-names id)
    (throw (ex-info "CSR artifact contains an unknown section id."
                    {:section-id id})))
  (when-not (contains? type-names type-id)
    (throw (ex-info "CSR artifact contains an unknown primitive type id."
                    {:section name
                     :type-id type-id})))
  (when (or (neg? offset)
            (neg? byte-length)
            (> (+ offset byte-length) file-size))
    (throw (ex-info "CSR artifact section extends outside the file."
                    {:section name
                     :offset offset
                     :byte-length byte-length
                     :file-size file-size})))
  (when-not (zero? (mod offset 8))
    (throw (ex-info "CSR artifact section is not 8-byte aligned."
                    {:section name
                     :offset offset})))
  (when (neg? count)
    (throw (ex-info "CSR artifact section has a negative count."
                    {:section name
                     :count count})))
  (when (and (contains? type-names type-id)
             (not= (checked-byte-length (type-names type-id) count) byte-length))
    (throw (ex-info "CSR artifact section byte length does not match its type and count."
                    {:section name
                     :type (type-names type-id)
                     :count count
                     :expected-byte-length (checked-byte-length (type-names type-id) count)
                     :actual-byte-length byte-length}))))

(defn- read-section-table [^ByteBuffer buffer file-size section-count section-table-offset]
  (when-not (= section-table-offset header-size)
    (throw (ex-info "Unsupported CSR artifact section table offset."
                    {:section-table-offset section-table-offset
                     :expected header-size})))
  (when (neg? section-count)
    (throw (ex-info "CSR artifact has a negative section count."
                    {:section-count section-count})))
  (let [table-end (+ section-table-offset (* section-count section-entry-size))]
    (when (> table-end file-size)
      (throw (ex-info "CSR artifact section table extends outside the file."
                      {:section-count section-count
                       :section-table-offset section-table-offset
                       :table-end table-end
                       :file-size file-size}))))
  (let [table-by-id (HashMap.)]
    (dotimes [idx section-count]
      (let [offset (+ section-table-offset (* idx section-entry-size))
            id (bb-get-int buffer offset)
            type-id (bb-get-int buffer (+ offset 4))
            count (bb-get-long buffer (+ offset 8))
            section-offset (bb-get-long buffer (+ offset 16))
            byte-length (bb-get-long buffer (+ offset 24))
            entry {:id id
                   :name (section-names id)
                   :type-id type-id
                   :type (type-names type-id)
                   :count count
                   :offset section-offset
                   :byte-length byte-length}]
        (validate-section! file-size entry)
        (when (.containsKey table-by-id id)
          (throw (ex-info "CSR artifact contains a duplicate section id."
                          {:section-id id
                           :section-name (section-names id)})))
        (.put table-by-id id entry)))
    (into {} table-by-id)))

(defn- parse-artifact [^ByteBuffer buffer file-size]
  (when (< file-size header-size)
    (throw (ex-info "CSR artifact is too small to contain a header."
                    {:file-size file-size
                     :header-size header-size})))
  (when (> file-size Integer/MAX_VALUE)
    (throw (ex-info "CSR artifact exceeds the current single-ByteBuffer reader limit."
                    {:file-size file-size
                     :max Integer/MAX_VALUE})))
  (let [magic ^"[B" artifact-magic]
    (dotimes [idx (alength magic)]
      (when-not (= (aget magic idx) (.get buffer idx))
        (throw (ex-info "CSR artifact magic mismatch."
                        {:offset idx})))))
  (let [version (bb-get-int buffer 8)
        marker (bb-get-int buffer 12)
        header-length (bb-get-int buffer 16)
        section-count (bb-get-int buffer 20)
        section-table-offset (bb-get-long buffer 24)
        row-count (bb-get-long buffer 32)
        col-count (bb-get-long buffer 40)
        entry-count (bb-get-long buffer 48)
        block-dim (bb-get-int buffer 56)
        flags (bb-get-int buffer 60)]
    (when-not (= version artifact-version)
      (throw (ex-info "Unsupported CSR artifact version."
                      {:version version
                       :supported artifact-version})))
    (when-not (= marker endian-marker)
      (throw (ex-info "CSR artifact endian marker mismatch."
                      {:endian-marker marker
                       :expected endian-marker})))
    (when-not (= header-length header-size)
      (throw (ex-info "Unsupported CSR artifact header length."
                      {:header-length header-length
                       :expected header-size})))
    (when (or (neg? row-count) (neg? col-count) (neg? entry-count) (not (pos? block-dim)))
      (throw (ex-info "CSR artifact contains invalid counts."
                      {:row-count row-count
                       :col-count col-count
                       :entry-count entry-count
                       :block-dim block-dim})))
    (let [sections (read-section-table buffer file-size section-count section-table-offset)]
      {:version version
       :flags flags
       :row-count row-count
       :col-count col-count
       :entry-count entry-count
       :block-dim block-dim
       :sections sections})))

(defn- validate-required-sections! [{:keys [row-count col-count entry-count block-dim sections]}]
  (let [row-ptrs (required-section sections :row-ptrs)
        col-ids (required-section sections :col-ids)
        payload-values (required-section sections :payload-values)
        row-key-offsets (required-section sections :row-key-offsets)
        row-key-bytes (required-section sections :row-key-bytes)
        col-key-offsets (required-section sections :col-key-offsets)
        col-key-bytes (required-section sections :col-key-bytes)
        metadata (required-section sections :metadata)]
    (doseq [[section expected-type] [[row-ptrs :int32]
                                     [col-ids :int32]
                                     [payload-values :float64]
                                     [row-key-offsets :int64]
                                     [row-key-bytes :bytes]
                                     [col-key-offsets :int64]
                                     [col-key-bytes :bytes]
                                     [metadata :utf8-json]]]
      (when-not (= expected-type (:type section))
        (throw (ex-info "CSR artifact section has the wrong primitive type."
                        {:section (:name section)
                         :expected expected-type
                         :actual (:type section)}))))
    (doseq [[section expected-count] [[row-ptrs (inc row-count)]
                                      [col-ids entry-count]
                                      [payload-values (* entry-count block-dim)]
                                      [row-key-offsets (inc row-count)]
                                      [col-key-offsets (inc col-count)]]]
      (when-not (= expected-count (:count section))
        (throw (ex-info "CSR artifact section has the wrong count."
                        {:section (:name section)
                         :expected expected-count
                         :actual (:count section)}))))
    {:row-ptrs row-ptrs
     :col-ids col-ids
     :payload-values payload-values
     :row-key-offsets row-key-offsets
     :row-key-bytes row-key-bytes
     :col-key-offsets col-key-offsets
     :col-key-bytes col-key-bytes
     :metadata metadata}))

(defn- validate-csr-arrays! [^ByteBuffer buffer {:keys [row-count col-count entry-count]} required]
  (let [row-ptrs (:row-ptrs required)
        col-ids (:col-ids required)
        row-ptrs-offset (:offset row-ptrs)
        col-ids-offset (:offset col-ids)]
    (when-not (zero? (bb-get-int buffer row-ptrs-offset))
      (throw (ex-info "CSR artifact row pointers must start at zero."
                      {:actual (bb-get-int buffer row-ptrs-offset)})))
    (loop [row-id 0
           prev (long 0)]
      (if (> row-id row-count)
        nil
        (let [ptr (bb-get-int buffer (+ row-ptrs-offset (* row-id 4)))]
          (when (or (neg? ptr) (> ptr entry-count) (< ptr prev))
            (throw (ex-info "CSR artifact row pointers are invalid."
                            {:row-id row-id
                             :previous prev
                             :actual ptr
                             :entry-count entry-count})))
          (recur (inc row-id) ptr))))
    (let [last-ptr (bb-get-int buffer (+ row-ptrs-offset (* row-count 4)))]
      (when-not (= entry-count last-ptr)
        (throw (ex-info "CSR artifact final row pointer must equal entry count."
                        {:final-row-ptr last-ptr
                         :entry-count entry-count}))))
    (dotimes [entry-id (int entry-count)]
      (let [col-id (bb-get-int buffer (+ col-ids-offset (* entry-id 4)))]
        (when (or (neg? col-id) (>= col-id col-count))
          (throw (ex-info "CSR artifact column id is out of bounds."
                          {:entry-id entry-id
                           :col-id col-id
                           :col-count col-count})))))))

(deftype MmapCSRSource
         [path
          ^MappedByteBuffer mapped
          sectionTable
          ^long rowCount
          ^long colCount
          ^long entryCount
          ^long payloadDim
          ^objects idToRow
          ^objects idToCol
          ^Map rowToId
          ^Map colToId
          ^long rowPtrsOffset
          ^long colIdsOffset
          ^long payloadValuesOffset
          rangeIndex]
  p/CSRSource
  (csr-row-count [_]
    rowCount)
  (csr-col-count [_]
    colCount)
  (csr-entry-count [_]
    entryCount)
  (csr-block-dim [_]
    payloadDim)
  (csr-row-id [_ row-key]
    (sparse/id-of rowToId row-key))
  (csr-col-id [_ col-key]
    (sparse/id-of colToId col-key))
  (csr-row-key-at [_ row-id]
    (sparse/key-of idToRow (ensure-index! :row-id row-id rowCount)))
  (csr-col-key-at [_ col-id]
    (sparse/key-of idToCol (ensure-index! :col-id col-id colCount)))
  (csr-row-span [_ row-id]
    (let [row-id (ensure-index! :row-id row-id rowCount)]
      [(bb-get-int mapped (+ rowPtrsOffset (* row-id 4)))
       (bb-get-int mapped (+ rowPtrsOffset (* (inc row-id) 4)))]))
  (csr-entry-col-id [_ entry-id]
    (let [entry-id (ensure-index! :entry-id entry-id entryCount)]
      (bb-get-int mapped (+ colIdsOffset (* entry-id 4)))))
  (csr-copy-block! [_ entry-id dst dst-off]
    (let [dst ^doubles dst
          entry-id (ensure-index! :entry-id entry-id entryCount)
          dst-off (ensure-copy-target! dst dst-off payloadDim)
          start (+ payloadValuesOffset (* entry-id payloadDim 8))]
      (dotimes [idx (int payloadDim)]
        (aset dst (+ (int dst-off) idx)
              (bb-get-double mapped (+ start (* idx 8)))))
      dst))
  (csr-scan-row! [this row-id visitor]
    (let [row-id* (int (ensure-index! :row-id row-id rowCount))
          [start end] (p/csr-row-span this row-id*)]
      (loop [entry-id start]
        (when (< entry-id end)
          (visitor row-id* (p/csr-entry-col-id this entry-id) entry-id this)
          (recur (inc entry-id)))))
    nil)
  (csr-resolve-ranges [_ selection]
    (cond
      (nil? selection) [(row-range 0 rowCount)]
      (:ranges selection) (:ranges selection)
      (:range selection) [(:range selection)]
      :else
      (let [prefix (normalize-prefix (:prefix selection))]
        (if (empty? prefix)
          [(row-range 0 rowCount)]
          (if rangeIndex
            (get rangeIndex prefix [])
            (throw (ex-info "CSRSource has no range index."
                            {:path path
                             :selection selection})))))))
  (csr-scan-ranges! [this ranges visitor]
    (doseq [r ranges]
      (let [[start end] (normalize-range r)]
        (loop [row-id start]
          (when (< row-id end)
            (p/csr-scan-row! this row-id visitor)
            (recur (inc row-id))))))
    nil))

(defn open-csr-artifact
  "Opens a language-neutral v1 CSR artifact as an mmap-backed CSRSource.

  The current reader maps the file as one Java ByteBuffer, so v1 artifacts must
  fit within Integer/MAX_VALUE bytes. The returned source owns no explicit file
  descriptor after open; the mmap remains live while the source is reachable and
  is released by the JVM."
  [path]
  (let [path (path-of path)]
    (with-open [channel (FileChannel/open path
                                          (into-array StandardOpenOption
                                                      [StandardOpenOption/READ]))]
      (let [file-size (.size channel)
            mapped ^MappedByteBuffer (.map channel FileChannel$MapMode/READ_ONLY 0 file-size)]
        (buffer-order! mapped)
        (let [artifact (parse-artifact mapped file-size)
              required (validate-required-sections! artifact)
              id-to-row (decode-key-section mapped
                                            (:row-key-offsets required)
                                            (:row-key-bytes required)
                                            (:row-count artifact))
              id-to-col (decode-key-section mapped
                                            (:col-key-offsets required)
                                            (:col-key-bytes required)
                                            (:col-count artifact))]
          (validate-csr-arrays! mapped artifact required)
          (->MmapCSRSource path
                           mapped
                           (:sections artifact)
                           (:row-count artifact)
                           (:col-count artifact)
                           (:entry-count artifact)
                           (:block-dim artifact)
                           id-to-row
                           id-to-col
                           (id-map id-to-row)
                           (id-map id-to-col)
                           (:offset (:row-ptrs required))
                           (:offset (:col-ids required))
                           (:offset (:payload-values required))
                           nil))))))

(defn csr-artifact-metadata
  "Returns parsed header and section metadata for a CSR artifact path or
  MmapCSRSource."
  [path-or-source]
  (if (instance? MmapCSRSource path-or-source)
    (let [source ^MmapCSRSource path-or-source]
      {:path (.-path source)
       :row-count (.-rowCount source)
       :col-count (.-colCount source)
       :entry-count (.-entryCount source)
       :block-dim (.-payloadDim source)
       :sections (.-sectionTable source)})
    (let [path (path-of path-or-source)]
      (with-open [channel (FileChannel/open path
                                            (into-array StandardOpenOption
                                                        [StandardOpenOption/READ]))]
        (let [file-size (.size channel)
              mapped ^MappedByteBuffer (.map channel FileChannel$MapMode/READ_ONLY 0 file-size)]
          (buffer-order! mapped)
          (let [artifact (parse-artifact mapped file-size)]
            (select-keys artifact [:version :flags :row-count :col-count :entry-count :block-dim :sections])))))))

(defn mmap-csr-source? [source]
  (instance? MmapCSRSource source))

(defn with-mmap-range-index [source _prefix-fn range-index]
  (->MmapCSRSource (.-path ^MmapCSRSource source)
                   (.-mapped ^MmapCSRSource source)
                   (.-sectionTable ^MmapCSRSource source)
                   (.-rowCount ^MmapCSRSource source)
                   (.-colCount ^MmapCSRSource source)
                   (.-entryCount ^MmapCSRSource source)
                   (.-payloadDim ^MmapCSRSource source)
                   (.-idToRow ^MmapCSRSource source)
                   (.-idToCol ^MmapCSRSource source)
                   (.-rowToId ^MmapCSRSource source)
                   (.-colToId ^MmapCSRSource source)
                   (.-rowPtrsOffset ^MmapCSRSource source)
                   (.-colIdsOffset ^MmapCSRSource source)
                   (.-payloadValuesOffset ^MmapCSRSource source)
                   range-index))
