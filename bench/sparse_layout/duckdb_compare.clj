(ns sparse-layout.duckdb-compare
  "Compares the CSR visitor with three Parquet encodings of 295-double blocks.

  Every timed path copies the same selected edges into caller-owned primitive
  arrays allocated before timing. Parquet generation and CSR construction are
  reported separately."
  (:gen-class)
  (:require [clojure.string :as str]
            [sparse-layout.bench-data :as data]
            [sparse-layout.core :as sparse]
            [sparse-layout.csr-source :as csr])
  (:import [com.sun.management ThreadMXBean]
           [java.lang.management ManagementFactory]
           [java.nio.file Files Path]
           [java.sql DriverManager]
           [java.util Arrays]
           [org.duckdb DuckDBAppender DuckDBChunkedResult DuckDBConnection DuckDBDataChunkReader
            DuckDBPreparedStatement DuckDBReadableVector]))

(def block-dim 295)

(sparse/defsparse comparison-features
                  {:row-key [:entity]
                   :cols-path [:vals]
                   :payload {:kind :fixed-double-block :dim block-dim}
                   :indices #{:csr}
                   :retain-coo? false})

(set! *warn-on-reflection* true)

(def ^:private ^ThreadMXBean thread-mx-bean
  (let [bean ^ThreadMXBean (cast ThreadMXBean (ManagementFactory/getThreadMXBean))]
    (when (and (.isThreadAllocatedMemorySupported bean)
               (not (.isThreadAllocatedMemoryEnabled bean)))
      (.setThreadAllocatedMemoryEnabled bean true))
    bean))

(defn- allocated-bytes
  ^long []
  (.getThreadAllocatedBytes thread-mx-bean (.getId (Thread/currentThread))))

(defn- timed
  [f]
  (let [before-bytes
        (allocated-bytes)

        start
        (System/nanoTime)

        value
        (f)]

    {:value value
     :ms (/ (double (- (System/nanoTime) start)) 1000000.0)
     :allocated-bytes (- (allocated-bytes) before-bytes)}))

(defn- fill-payload!
  [^doubles payload ^long row-id ^long raw-col-id]
  (let [base (+ (* row-id 0.001) (* raw-col-id 0.01))]
    (dotimes [lane block-dim]
      (aset payload lane (+ base lane))))
  payload)

(defn- raw-col->id
  ^ints [{:keys [row-count col-count nnz-per-row]}]
  (let [ids
        (int-array col-count)

        next-id
        (int-array 1)]

    (Arrays/fill ids -1)
    (dotimes [row-id row-count]
      (doseq [raw-col-id (data/row-col-ids row-id col-count nnz-per-row)]
        (when (= -1 (aget ids raw-col-id))
          (aset ids raw-col-id (aget next-id 0))
          (aset next-id 0 (inc (aget next-id 0))))))
    ids))

(defn- sql-path [^Path path] (str "'" (str/replace (.toString path) "'" "''") "'"))

(defn- payload-columns
  [payload-expression]
  (str/join ", "
            (map (fn [lane]
                   (str payload-expression "[" (inc lane) "] AS p" lane))
                 (range block-dim))))

(defn- execute!
  [^DuckDBConnection connection sql]
  (with-open [statement (.createStatement connection)]
    (.execute statement sql)))

(defn- open-duckdb
  ^DuckDBConnection []
  (let [connection (-> (DriverManager/getConnection "jdbc:duckdb:")
                       (.unwrap DuckDBConnection))]
    (execute! connection "SET threads = 1")
    (execute! connection "SET preserve_insertion_order = true")
    connection))

(defn- duckdb-version
  []
  (with-open [connection
              (open-duckdb)

              statement
              (.createStatement connection)

              result
              (.executeQuery statement "SELECT version()")]

    (.next result)
    (.getString result 1)))

(defn- append-base!
  [^DuckDBConnection connection {:keys [row-count col-count nnz-per-row]} ^ints col-ids]
  (execute! connection
            (str "CREATE TABLE base_ingest "
                 "(row_id INTEGER, col_id INTEGER, payload DOUBLE["
                 block-dim
                 "] NOT NULL)"))
  (let [payload (double-array block-dim)]
    (with-open [appender ^DuckDBAppender (.createAppender connection "base_ingest")]
      (dotimes [row-id row-count]
        (doseq [raw-col-id (data/row-col-ids row-id col-count nnz-per-row)]
          (fill-payload! payload row-id raw-col-id)
          (.beginRow appender)
          (.append appender (int row-id))
          (.append appender (int (aget col-ids raw-col-id)))
          (.append appender payload)
          (.endRow appender)))))
  (execute! connection "CREATE TABLE base AS SELECT * FROM base_ingest ORDER BY row_id, col_id")
  (execute! connection "DROP TABLE base_ingest"))

(defn- copy-parquet!
  [^DuckDBConnection connection query ^Path path]
  (execute! connection
            (str "COPY (" query ") TO " (sql-path path) " (FORMAT PARQUET, COMPRESSION ZSTD)")))

(defn- parquet-read-type
  [^DuckDBConnection connection ^Path path]
  (with-open [statement
              (.createStatement connection)

              result
              (.executeQuery
                statement
                (str "DESCRIBE SELECT payload FROM read_parquet(" (sql-path path) ")"))]

    (.next result)
    (.getString result 2)))

(defn- generate-parquet!
  [config ^ints col-ids ^Path directory]
  (let [wide
        (.resolve directory "column-explosion.parquet")

        long
        (.resolve directory "row-explosion.parquet")

        array
        (.resolve directory "array-list.parquet")]

    (with-open [connection (open-duckdb)]
      (let [base (timed #(append-base! connection config col-ids))
            wide-result (timed #(copy-parquet! connection
                                               (str "SELECT row_id, col_id, "
                                                    (payload-columns "payload")
                                                    " FROM base ORDER BY row_id, col_id")
                                               wide))
            long-result (timed #(copy-parquet!
                                  connection
                                  (str "SELECT row_id, col_id, CAST(lane AS INTEGER) AS lane, "
                                       "payload[lane + 1] AS value "
                                       "FROM base, range(0, " block-dim
                                       ") AS lanes(lane) " "ORDER BY row_id, col_id, lane")
                                  long))
            array-result (timed
                           #(copy-parquet!
                              connection
                              "SELECT row_id, col_id, payload FROM base ORDER BY row_id, col_id"
                              array))
            array-read-type (parquet-read-type connection array)]

        {:paths {:column-explosion wide :row-explosion long :array-list array}
         :setup {:duckdb-base base
                 :column-explosion (assoc wide-result :bytes (Files/size wide))
                 :row-explosion (assoc long-result :bytes (Files/size long))
                 :array-list (assoc array-result :bytes (Files/size array))}
         :array-read-type array-read-type}))))

(defn- destination
  [^long edge-count]
  {:rows (int-array edge-count)
   :cols (int-array edge-count)
   :values (double-array (* edge-count block-dim))})

(defn- sparse-visitor-runner
  [source ^long start-row ^long end-row destination]
  (let [rows
        ^ints (:rows destination)

        cols
        ^ints (:cols destination)

        values
        ^doubles (:values destination)

        cursor
        (int-array 1)

        ranges
        [{:row-start (int start-row) :row-end (int end-row)}]

        visitor
        (fn [row-id col-id entry-id src]
          (let [idx (aget cursor 0)]
            (aset rows idx (int row-id))
            (aset cols idx (int col-id))
            (csr/csr-copy-block! src entry-id values (* idx block-dim))
            (aset cursor 0 (inc idx))))]

    (fn []
      (aset cursor 0 0)
      (csr/csr-scan-ranges! source ranges visitor)
      (aget cursor 0))))

(defn- sparse-bulk-runner
  [source ^long start-row ^long end-row destination]
  (let [ranges
        [{:row-start (int start-row) :row-end (int end-row)}]

        rows
        ^ints (:rows destination)

        cols
        ^ints (:cols destination)

        values
        ^doubles (:values destination)]

    #(csr/csr-copy-ranges! source ranges rows cols values 0)))

(defn- parquet-query
  [^Path path projection]
  (str "SELECT " projection
       " FROM read_parquet(" (sql-path path)
       ")" " WHERE row_id >= ? AND row_id < ?"))

(defn- array-projection
  []
  (str "row_id, col_id, " (payload-columns (str "(payload::DOUBLE[" block-dim "])"))))

(defn- parquet->sparse-source
  [{:keys [row-count col-count] :as config} ^ints col-ids ^Path path]
  (let [row-key
        (data/row-key-fn config)

        col-key
        (data/col-key-fn config)

        rows
        (mapv row-key (range row-count))

        cols
        (mapv col-key (range col-count))

        raw-col-ids
        (int-array col-count)

        builder
        (make-comparison-features-builder)

        payload
        (double-array block-dim)

        payload-vectors
        (object-array block-dim)

        count
        (int-array 1)]

    (dotimes [raw-col-id col-count]
      (let [col-id (aget col-ids raw-col-id)]
        (when-not (= -1 col-id) (aset raw-col-ids col-id raw-col-id))))
    (with-open [connection
                (open-duckdb)

                statement
                ^DuckDBPreparedStatement
                (.prepare
                  connection
                  (str "SELECT " (array-projection) " FROM read_parquet(" (sql-path path) ")"))

                result
                ^DuckDBChunkedResult (.query statement)]

      (while (.nextChunk result)
        (let [chunk
              ^DuckDBDataChunkReader (.chunk result)

              chunk-rows
              (int (.rowCount chunk))

              row-vector
              ^DuckDBReadableVector (.vector chunk 0)

              col-vector
              ^DuckDBReadableVector (.vector chunk 1)]

          (dotimes [lane block-dim]
            (aset payload-vectors lane (.vector chunk (+ lane 2))))
          (dotimes [chunk-row chunk-rows]
            (let [row-id (.getInt row-vector chunk-row)
                  col-id (.getInt col-vector chunk-row)
                  raw-col-id (aget raw-col-ids col-id)]

              (dotimes [lane block-dim]
                (let [vector ^DuckDBReadableVector (aget payload-vectors lane)]
                  (aset payload lane (.getDouble vector chunk-row))))
              (sparse/append-entry! builder (nth rows row-id) (nth cols raw-col-id) payload)
              (aset count 0 (inc (aget count 0))))))))
    (assert (= (data/nnz config) (aget count 0)))
    (csr/dataset->csr-source (comparison-features-freeze! builder))))

(defn- projected-runner
  [^DuckDBConnection connection ^Path path projection start-row end-row destination]
  (let [statement
        ^DuckDBPreparedStatement (.prepare connection (parquet-query path projection))

        rows
        ^ints (:rows destination)

        cols
        ^ints (:cols destination)

        values
        ^doubles (:values destination)

        payload-vectors
        (object-array block-dim)]

    (fn []
      (.setInt statement 1 (int start-row))
      (.setInt statement 2 (int end-row))
      (with-open [result ^DuckDBChunkedResult (.query statement)]
        (loop [out 0]
          (if (.nextChunk result)
            (let [chunk ^DuckDBDataChunkReader (.chunk result)
                  chunk-rows (int (.rowCount chunk))
                  row-vector ^DuckDBReadableVector (.vector chunk 0)
                  col-vector ^DuckDBReadableVector (.vector chunk 1)]

              (dotimes [lane block-dim]
                (aset payload-vectors lane (.vector chunk (+ lane 2))))
              (dotimes [chunk-row chunk-rows]
                (let [idx (+ out chunk-row)
                      payload-offset (* idx block-dim)]

                  (aset rows idx (.getInt row-vector chunk-row))
                  (aset cols idx (.getInt col-vector chunk-row))
                  (dotimes [lane block-dim]
                    (let [vector ^DuckDBReadableVector (aget payload-vectors lane)]
                      (aset values (+ payload-offset lane) (.getDouble vector chunk-row))))))
              (recur (+ out chunk-rows)))
            out))))))

(defn- long-runner
  [^DuckDBConnection connection ^Path path start-row end-row destination]
  (let [statement
        ^DuckDBPreparedStatement
        (.prepare connection (parquet-query path "row_id, col_id, lane, value"))

        rows
        ^ints (:rows destination)

        cols
        ^ints (:cols destination)

        values
        ^doubles (:values destination)]

    (fn []
      (.setInt statement 1 (int start-row))
      (.setInt statement 2 (int end-row))
      (with-open [result ^DuckDBChunkedResult (.query statement)]
        (loop [scalar-out 0]
          (if (.nextChunk result)
            (let [chunk ^DuckDBDataChunkReader (.chunk result)
                  chunk-rows (int (.rowCount chunk))
                  row-vector ^DuckDBReadableVector (.vector chunk 0)
                  col-vector ^DuckDBReadableVector (.vector chunk 1)
                  lane-vector ^DuckDBReadableVector (.vector chunk 2)
                  value-vector ^DuckDBReadableVector (.vector chunk 3)]

              (dotimes [chunk-row chunk-rows]
                (let [idx (+ scalar-out chunk-row)
                      lane (.getInt lane-vector chunk-row)]

                  (when (zero? lane)
                    (let [edge (quot idx block-dim)]
                      (aset rows edge (.getInt row-vector chunk-row))
                      (aset cols edge (.getInt col-vector chunk-row))))
                  (aset values idx (.getDouble value-vector chunk-row))))
              (recur (+ scalar-out chunk-rows)))
            (quot scalar-out block-dim)))))))

(defn- same-output?
  [expected actual]
  (and (Arrays/equals ^ints (:rows expected) ^ints (:rows actual))
       (Arrays/equals ^ints (:cols expected) ^ints (:cols actual))
       (Arrays/equals ^doubles (:values expected) ^doubles (:values actual))))

(defn- percentile
  ^long [^longs sorted-values percentile]
  (let [idx (dec (long (Math/ceil (* percentile (alength sorted-values)))))]
    (aget sorted-values (max 0 idx))))

(defn- measure
  [f expected-count reps]
  (dotimes [_ (min 2 reps)]
    (assert (= expected-count (long (f)))))
  (let [times
        (long-array reps)

        allocations
        (long-array reps)]

    (dotimes [idx reps]
      (let [before-bytes (allocated-bytes)
            start (System/nanoTime)
            count (long (f))
            elapsed (- (System/nanoTime) start)
            bytes (- (allocated-bytes) before-bytes)]

        (assert (= expected-count count))
        (aset times idx elapsed)
        (aset allocations idx bytes)))
    (Arrays/sort times)
    (Arrays/sort allocations)
    {:median-ns (aget times (quot reps 2))
     :p95-ns (percentile times 0.95)
     :median-allocated-bytes (aget allocations (quot reps 2))
     :reps reps}))

(defn- selection-specs
  [{:keys [row-count]}]
  (let [middle
        (quot row-count 2)

        range-size
        (min 64 (max 2 (quot row-count 2)))

        range-start
        (min middle (- row-count range-size))]

    [{:selection :one-row :start middle :end (inc middle)}
     {:selection :rows-64 :start range-start :end (+ range-start range-size)}
     {:selection :full :start 0 :end row-count}]))

(defn- reps-for
  [edge-count smoke?]
  (if smoke? 1 (max 5 (min 30 (quot 3000000 (max 1 (* edge-count block-dim)))))))

(defn- format-bytes
  [bytes]
  (let [units ["B" "KiB" "MiB" "GiB"]]
    (loop [value (double bytes)
           unit 0]

      (if (or (< value 1024.0) (= unit (dec (count units))))
        (format "%.2f %s" value (nth units unit))
        (recur (/ value 1024.0) (inc unit))))))

(defn- print-setup
  [{:keys [setup array-read-type]} sparse-build]
  (println (format "%-29s %12s %14s" "setup" "time" "JVM alloc/file"))
  (println (format "%-29s %10.1f ms %14s"
                   "Parquet list -> COO -> CSR"
                   (:ms sparse-build)
                   (format-bytes (:allocated-bytes sparse-build))))
  (println (format "%-29s %10.1f ms %14s"
                   "DuckDB normalized ARRAY table"
                   (get-in setup [:duckdb-base :ms])
                   (format-bytes (get-in setup [:duckdb-base :allocated-bytes]))))
  (doseq [[label key] [["Parquet: 295 columns" :column-explosion]
                       ["Parquet: 295 rows/edge" :row-explosion]
                       ["Parquet: 295-value list" :array-list]]]
    (println (format "%-29s %10.1f ms %14s"
                     label
                     (get-in setup [key :ms])
                     (format-bytes (get-in setup [key :bytes])))))
  (println "DuckDB type after Parquet round-trip:" array-read-type))

(defn- print-result
  [{:keys [selection backend entries median-ns p95-ns median-allocated-bytes reps]}]
  (println (format "%-10s %-24s %,9d %10.3f %10.3f %12s %5d"
                   (name selection)
                   (name backend)
                   entries
                   (/ median-ns 1000000.0)
                   (/ p95-ns 1000000.0)
                   (format-bytes median-allocated-bytes)
                   reps)))

(defn- run-selection
  [^DuckDBConnection connection source paths config selection smoke?]
  (let [{:keys [start end]}
        selection

        edge-count
        (* (- end start) (:nnz-per-row config))

        expected
        (destination edge-count)

        actual
        (destination edge-count)

        projection
        (array-projection)

        wide-projection
        (str "row_id, col_id, " (str/join ", " (map #(str "p" %) (range block-dim))))

        runners
        [[:sparse-bulk (sparse-bulk-runner source start end expected) expected]
         [:sparse-visitor (sparse-visitor-runner source start end actual) actual]
         [:parquet-295-columns
          (projected-runner connection (:column-explosion paths) wide-projection start end actual)
          actual]
         [:parquet-295-rows (long-runner connection (:row-explosion paths) start end actual) actual]
         [:parquet-list-as-array
          (projected-runner connection (:array-list paths) projection start end actual) actual]]

        baseline-count
        ((second (first runners)))]

    (assert (= edge-count baseline-count))
    (doseq [[backend runner output] (rest runners)]
      (assert (= edge-count (long (runner))) (str backend " returned the wrong edge count"))
      (assert (same-output? expected output) (str backend " output differs from sparse CSR")))
    (let [reps (reps-for edge-count smoke?)]
      (mapv (fn [[backend runner _]]
              (merge selection
                     {:backend backend :entries edge-count}
                     (measure runner edge-count reps)))
            runners))))

(defn- delete-tree!
  [^Path directory]
  (when directory
    (with-open [paths (Files/list directory)]
      (doseq [path (iterator-seq (.iterator paths))]
        (Files/deleteIfExists ^Path path)))
    (Files/deleteIfExists directory)))

(defn- run-config
  [config smoke?]
  (println)
  (println "===" (:label config) "===")
  (println "Config:"
           (select-keys (assoc config
                          :rows (:row-count config)
                          :cols (:col-count config)
                          :nnz (data/nnz config)
                          :block-dim block-dim)
                        [:key-shape :scale :rows :cols :nnz-per-row :nnz :block-dim]))
  (println "Logical payload:" block-dim "doubles per (row key, column key)")
  (println "Logical payload bytes:" (format-bytes (* (data/nnz config) block-dim 8)))
  (let [directory (Files/createTempDirectory "sparse-layout-duckdb-"
                                             (make-array java.nio.file.attribute.FileAttribute 0))]
    (try (let [col-ids (raw-col->id config)
               parquet (generate-parquet! config col-ids directory)
               sparse-build (timed #(parquet->sparse-source config
                                                            col-ids
                                                            (get-in parquet [:paths :array-list])))
               source (:value sparse-build)]

           (assert (= block-dim (csr/csr-block-dim source)))
           (assert (= (data/nnz config) (csr/csr-entry-count source)))
           (print-setup parquet sparse-build)
           (println)
           (println (format "%-10s %-24s %9s %10s %10s %12s %5s" "selection"
                            "backend" "edges"
                            "median ms" "p95 ms"
                            "alloc/call" "reps"))
           (with-open [connection (open-duckdb)]
             (let [results
                   (vec (mapcat #(run-selection connection source (:paths parquet) config % smoke?)
                                (selection-specs config)))]
               (doseq [result results]
                 (print-result result))
               {:config config
                :setup (dissoc (:setup parquet) :value)
                :array-read-type (:array-read-type parquet)
                :results results})))
         (finally (delete-tree! directory)))))

(defn- smoke-config
  []
  (assoc (first (filter #(= :compound-map (:key-shape %)) data/benchmark-configs))
    :label "compound map keys / smoke"
    :scale :smoke
    :row-count 32
    :col-count 64
    :nnz-per-row 8))

(defn run
  [args]
  (let [smoke?
        (some #{"smoke"} args)

        selection-args
        (if (some #{"scalar" "compound"} args) args (conj (vec args) "compound"))

        configs
        (if smoke? [(smoke-config)] (data/selected-configs selection-args))]

    (println "DuckDB" (duckdb-version) "vs sparse-layout CSR")
    (println "Three Parquet shapes; outputs are reused int[]/double[] destinations.")
    (println
      "DuckDB chunked results cannot expose nested columns, so the list is projected to doubles.")
    (mapv #(run-config % smoke?) configs)))

(defn -main [& args] (run args))
