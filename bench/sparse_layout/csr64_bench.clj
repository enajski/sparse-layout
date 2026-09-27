(ns sparse-layout.csr64-bench
  "Matched heap CSR and Java 25 MemorySegment CSR64 benchmark."
  (:gen-class)
  (:require [sparse-layout.csr-source :as csr]
            [sparse-layout.csr-source.protocols :as p]
            [sparse-layout.csr64 :as csr64]
            [sparse-layout.csr64-overlay :as overlay])
  (:import [com.sun.management ThreadMXBean]
           [java.lang.management ManagementFactory]
           [java.nio.file Files Path]
           [java.util Arrays HashMap SplittableRandom]))

(set! *warn-on-reflection* true)

(def block-dim 295)
(def latency-ceiling-ms 50.0)

(def configs
  {:smoke
   {:label "smoke" :row-count 256 :col-count 64 :entries-per-row 8 :block-dim block-dim :reps 3}
   :medium {:label "medium / matched historical shape"
            :row-count 16384
            :col-count 1024
            :entries-per-row 16
            :block-dim block-dim
            :reps 100}
   :ceiling {:label "near maximum Java primitive-array element count"
             :row-count 28547
             :col-count 1024
             :entries-per-row 255
             :block-dim block-dim
             :reps 100}})

(defn- range-start ^long [r] (if (map? r) (long (:row-start r)) (long (nth r 0))))

(defn- range-end ^long [r] (if (map? r) (long (:row-end r)) (long (nth r 1))))

(deftype UniformCSRSource [^long rowCount ^long colCount ^long entriesPerRow ^long blockDim]
  p/CSRSource
    (csr-row-count [_] rowCount)
    (csr-col-count [_] colCount)
    (csr-entry-count [_] (* rowCount entriesPerRow))
    (csr-block-dim [_] blockDim)
    (csr-row-id [_ row-key]
      (if (and (integer? row-key) (<= 0 (long row-key)) (< (long row-key) rowCount))
        (long row-key)
        -1))
    (csr-col-id [_ col-key]
      (if (and (integer? col-key) (<= 0 (long col-key)) (< (long col-key) colCount))
        (long col-key)
        -1))
    (csr-row-key-at [_ row-id] row-id)
    (csr-col-key-at [_ col-id] col-id)
    (csr-row-span [_ row-id] [(* (long row-id) entriesPerRow)
                              (* (inc (long row-id)) entriesPerRow)])
    (csr-entry-col-id [_ entry-id] (mod (long entry-id) entriesPerRow))
    (csr-copy-block! [_ _entry-id dst dst-off]
      (Arrays/fill ^doubles dst (int dst-off) (int (+ (long dst-off) blockDim)) 1.0)
      dst)
    (csr-copy-ranges! [_ ranges dst-row-ids dst-col-ids dst-values dst-entry-off]
      (let [dst-row-ids
            ^ints dst-row-ids

            dst-col-ids
            ^ints dst-col-ids

            dst-values
            ^doubles dst-values

            total
            (* entriesPerRow
               (reduce (fn [sum r]
                         (+ sum (- (range-end r) (range-start r))))
                       0
                       ranges))

            entry-end
            (+ (long dst-entry-off) total)

            value-end
            (* entry-end blockDim)]

        (when (or (> entry-end (alength dst-row-ids))
                  (> entry-end (alength dst-col-ids))
                  (> value-end (alength dst-values)))
          (throw (ex-info "Uniform source destination is too small." {:entries total})))
        (loop [remaining
               (seq ranges)

               copied
               (long 0)]

          (if-let [r (first remaining)]
            (let [start (range-start r)
                  end (range-end r)
                  range-entries (* (- end start) entriesPerRow)
                  range-dst (+ (long dst-entry-off) copied)]

              (loop [row-id start]
                (when (< row-id end)
                  (let [row-dst (+ range-dst (* (- row-id start) entriesPerRow))]
                    (Arrays/fill dst-row-ids
                                 (int row-dst)
                                 (int (+ row-dst entriesPerRow))
                                 (int row-id))
                    (dotimes [entry (int entriesPerRow)]
                      (aset dst-col-ids (+ (int row-dst) entry) entry))
                    (recur (inc row-id)))))
              (Arrays/fill dst-values
                           (int (* range-dst blockDim))
                           (int (* (+ range-dst range-entries) blockDim))
                           1.0)
              (recur (next remaining) (+ copied range-entries)))
            copied))))
    (csr-scan-row! [this row-id visitor]
      (let [start
            (* (long row-id) entriesPerRow)

            end
            (+ start entriesPerRow)]

        (loop [entry-id start]
          (when (< entry-id end)
            (visitor row-id (mod entry-id entriesPerRow) entry-id this)
            (recur (inc entry-id)))))
      nil)
    (csr-resolve-ranges [_ selection]
      (cond (nil? selection) [[0 rowCount]]
            (:ranges selection) (:ranges selection)
            (:range selection) [(:range selection)]
            :else (throw (ex-info "Uniform source has no prefix index." {:selection selection}))))
    (csr-scan-ranges! [this ranges visitor]
      (doseq [r ranges]
        (loop [row-id (range-start r)]
          (when (< row-id (range-end r))
            (p/csr-scan-row! this row-id visitor)
            (recur (inc row-id)))))
      nil))

(defn- uniform-source
  [{:keys [row-count col-count entries-per-row block-dim]}]
  (->UniformCSRSource row-count col-count entries-per-row block-dim))

(defn- heap-source
  [{:keys [row-count col-count entries-per-row block-dim] :as config}]
  (let [entry-count
        (* row-count entries-per-row)

        value-count
        (* entry-count block-dim)]

    (when (> value-count (- Integer/MAX_VALUE 8))
      (throw (ex-info "Heap comparator would exceed one double array." {:config config})))
    (let [row-ptrs
          (int-array (inc row-count))

          col-ids
          (int-array entry-count)

          values
          (double-array value-count)]

      (dotimes [row-id row-count]
        (aset row-ptrs row-id (int (* row-id entries-per-row)))
        (dotimes [entry entries-per-row]
          (aset col-ids (+ (* row-id entries-per-row) entry) entry)))
      (aset row-ptrs row-count (int entry-count))
      (Arrays/fill values 1.0)
      (csr/->HeapCSRSource (HashMap.)
                           (object-array row-count)
                           (HashMap.)
                           (object-array col-count)
                           row-ptrs
                           col-ids
                           values
                           block-dim
                           nil
                           nil))))

(defn- destination
  [entry-count block-dim]
  {:rows (int-array entry-count)
   :cols (int-array entry-count)
   :values (double-array (* entry-count block-dim))})

(defn- same-output?
  [expected actual]
  (and (Arrays/equals ^ints (:rows expected) ^ints (:rows actual))
       (Arrays/equals ^ints (:cols expected) ^ints (:cols actual))
       (Arrays/equals ^doubles (:values expected) ^doubles (:values actual))))

(def ^:private ^ThreadMXBean thread-mx-bean
  (let [bean ^ThreadMXBean (cast ThreadMXBean (ManagementFactory/getThreadMXBean))]
    (when (and (.isThreadAllocatedMemorySupported bean)
               (not (.isThreadAllocatedMemoryEnabled bean)))
      (.setThreadAllocatedMemoryEnabled bean true))
    bean))

(defn- allocated-bytes
  ^long []
  (.getThreadAllocatedBytes thread-mx-bean (.getId (Thread/currentThread))))

(defn- percentile
  ^long [^longs sorted-values ^double percentile]
  (let [index (dec (long (Math/ceil (* percentile (alength sorted-values)))))]
    (aget sorted-values (max 0 index))))

(defn- measure
  [f expected-count reps]
  (dotimes [_ 3]
    (assert (= expected-count (long (f)))))
  (let [times
        (long-array reps)

        allocations
        (long-array reps)]

    (dotimes [index reps]
      (let [before-bytes (allocated-bytes)
            start (System/nanoTime)
            actual (long (f))
            elapsed (- (System/nanoTime) start)]

        (assert (= expected-count actual))
        (aset times index elapsed)
        (aset allocations index (- (allocated-bytes) before-bytes))))
    (Arrays/sort times)
    (Arrays/sort allocations)
    {:median-ns (aget times (quot reps 2))
     :p95-ns (percentile times 0.95)
     :p99-ns (percentile times 0.99)
     :median-allocated-bytes (aget allocations (quot reps 2))
     :reps reps}))

(defn- timed
  [f]
  (let [start
        (System/nanoTime)

        value
        (f)]

    {:value value :ms (/ (- (System/nanoTime) start) 1000000.0)}))

(defn- selections
  [{:keys [row-count entries-per-row]} ceiling?]
  (let [middle
        (quot row-count 2)

        rows-64-start
        (- middle 32)

        page-rows
        (min row-count (quot 226719 entries-per-row))]

    (cond-> [{:label :one-row :start middle :end (inc middle)}
             {:label :rows-64 :start rows-64-start :end (+ rows-64-start 64)}]
      ceiling?
      (conj {:label :page-512-mib :start (- row-count page-rows) :end row-count})

      (not ceiling?)
      (conj {:label :full :start 0 :end row-count}))))

(defn- runner
  [backend source start end entry-count destination]
  (let [rows
        ^ints (:rows destination)

        cols
        ^ints (:cols destination)

        values
        ^doubles (:values destination)]

    (case backend
      :heap
      #(csr/csr-copy-ranges! source [[start end]] rows cols values 0)

      :csr64
      #(csr64/copy-page! source start end 0 entry-count rows cols values 0))))

(defn- result
  [backend selection stats]
  (let [p99-ms (/ (:p99-ns stats) 1000000.0)]
    (merge {:backend backend
            :selection (:label selection)
            :entries (:entries selection)
            :p99-under-50ms? (<= p99-ms latency-ceiling-ms)}
           stats)))

(defn- print-result
  [{:keys [backend selection entries median-ns p95-ns p99-ns median-allocated-bytes
           p99-under-50ms?]}]
  (println (format "%-8s %-13s %,9d %10.3f %10.3f %10.3f %,12d %s"
                   (name backend)
                   (name selection)
                   entries
                   (/ median-ns 1000000.0)
                   (/ p95-ns 1000000.0)
                   (/ p99-ns 1000000.0)
                   median-allocated-bytes
                   (if p99-under-50ms? "PASS" "FAIL"))))

(defn- run-selection
  [mapped heap config selection]
  (let [{:keys [start end]}
        selection

        entry-count
        (* (- end start) (:entries-per-row config))

        selection
        (assoc selection :entries entry-count)

        mapped-output
        (destination entry-count (:block-dim config))

        mapped-runner
        (runner :csr64 mapped start end entry-count mapped-output)

        heap-output
        (when heap (destination entry-count (:block-dim config)))

        heap-runner
        (when heap (runner :heap heap start end entry-count heap-output))]

    (when heap
      (assert (= entry-count (long (heap-runner))))
      (assert (= entry-count (long (mapped-runner))))
      (assert (same-output? heap-output mapped-output)))
    (let [mapped-result
          (result :csr64 selection (measure mapped-runner entry-count (:reps config)))

          rows
          ^ints (:rows mapped-output)

          cols
          ^ints (:cols mapped-output)

          values
          ^doubles (:values mapped-output)]

      (assert (= start (aget rows 0)))
      (assert (= (dec end) (aget rows (dec entry-count))))
      (assert (= 0 (aget cols 0)))
      (assert (= (dec (:entries-per-row config)) (aget cols (dec entry-count))))
      (assert (= 1.0 (aget values 0)))
      (assert (= 1.0 (aget values (dec (alength values)))))
      (cond-> [mapped-result]
        heap
        (conj (result :heap selection (measure heap-runner entry-count (:reps config))))))))

(defn- temporary-artifact
  ^Path []
  (Files/createTempFile "sparse-layout-csr64-bench-"
                        ".slcsr64"
                        (make-array java.nio.file.attribute.FileAttribute 0)))

(defn run-config
  [config]
  (let [ceiling?
        (= "near maximum Java primitive-array element count" (:label config))

        generated
        (uniform-source config)

        path
        (temporary-artifact)]

    (try
      (let [write-result
            (timed #(csr64/write-artifact! generated path {:page-entries 16320}))

            heap-result
            (when-not ceiling? (timed #(heap-source config)))

            open-result
            (timed #(csr64/open-artifact path))

            mapped
            (:value open-result)

            heap
            (:value heap-result)

            payload-elements
            (* (p/csr-entry-count generated) (:block-dim config))]

        (try (let [load-result (timed #(csr64/load! mapped))]
               (println)
               (println (:label config))
               (println {:java (System/getProperty "java.runtime.version")
                         :rows (:row-count config)
                         :entries (p/csr-entry-count generated)
                         :block-dim (:block-dim config)
                         :payload-elements payload-elements
                         :payload-gib (/ (* 8.0 payload-elements) 1073741824.0)
                         :artifact-gib (/ (Files/size path) 1073741824.0)
                         :write-ms (:ms write-result)
                         :open-ms (:ms open-result)
                         :load-ms (:ms load-result)
                         :loaded-hint (csr64/loaded? mapped)
                         :heap-build-ms (:ms heap-result)})
               (println (format "%-8s %-13s %9s %10s %10s %10s %12s %s"
                                "backend" "selection"
                                "entries" "median ms"
                                "p95 ms" "p99 ms"
                                "alloc bytes" "<50ms"))
               (let [results (vec (mapcat #(run-selection mapped heap config %)
                                          (selections config ceiling?)))]
                 (doseq [item results]
                   (print-result item))
                 (when-let [failures (seq (filter #(and (= :csr64 (:backend %))
                                                        (not (:p99-under-50ms? %)))
                                                  results))]
                   (throw (ex-info "CSR64 latency ceiling failed." {:failures failures})))
                 results))
             (finally (.close ^java.io.Closeable mapped))))
      (finally (Files/deleteIfExists path)))))

(defn- replacement-ledger
  [{:keys [row-count entries-per-row]} modification-count]
  (let [block (double-array block-dim)]
    (Arrays/fill block 2.0)
    (mapv (fn [sequence-id]
            {:sequence sequence-id
             :op :put
             :row-id (mod sequence-id row-count)
             :col-id (quot sequence-id row-count)
             :block block})
          (range (min modification-count (* row-count entries-per-row))))))

(defn- structural-ledger
  [{:keys [row-count entries-per-row]} dirty-row-count]
  (let [block (double-array block-dim)]
    (Arrays/fill block 3.0)
    (vec (mapcat (fn [row-id]
                   [{:sequence (* row-id 2) :op :delete :row-id row-id :col-id 0}
                    {:sequence (inc (* row-id 2))
                     :op :put
                     :row-id row-id
                     :col-id entries-per-row
                     :block block}])
                 (range (min dirty-row-count row-count))))))

(defn- overlay-selections
  [{:keys [row-count]}]
  [{:label :one-row :start 0 :end 1} {:label :rows-64 :start 0 :end (min 64 row-count)}
   {:label :full :start 0 :end row-count}])

(defn- run-overlay-selection
  [view config scenario selection]
  (let [{:keys [start end]}
        selection

        entry-count
        (overlay/range-entry-count view start end)

        output
        (destination entry-count (:block-dim config))

        rows
        ^ints (:rows output)

        cols
        ^ints (:cols output)

        values
        ^doubles (:values output)

        runner
        #(overlay/copy-page! view start end 0 entry-count rows cols values 0)

        stats
        (measure runner entry-count (:reps config))

        item
        (result scenario (assoc selection :entries entry-count) stats)]

    (assert (= start (aget rows 0)))
    (assert (= (dec end) (aget rows (dec entry-count))))
    (case scenario
      :replacements-100k
      (assert (= 2.0 (aget values 0)))

      :structural-1000-rows
      (do (assert (= 1 (aget cols 0)))
          (assert (= (:entries-per-row config) (aget cols (dec (:entries-per-row config)))))))
    item))

(defn- run-overlay-scenario
  [mapped config scenario modifications]
  (let [compile-result
        (timed #(overlay/compile-ledger mapped modifications))

        view
        (:value compile-result)

        results
        (mapv #(run-overlay-selection view config scenario %) (overlay-selections config))]

    (println)
    (println (name scenario) (assoc (overlay/metadata view) :compile-ms (:ms compile-result)))
    (doseq [item results]
      (print-result item))
    (when-let [failures (seq (remove :p99-under-50ms? results))]
      (throw (ex-info "CSR64 ledger overlay latency ceiling failed." {:failures failures})))
    results))

(defn run-overlay-config
  [config]
  (let [generated
        (uniform-source config)

        path
        (temporary-artifact)]

    (try
      (csr64/write-artifact! generated path {:page-entries 16320})
      (with-open [mapped (csr64/open-artifact path)]
        (csr64/load! mapped)
        (println)
        (println "CSR64 modification-ledger overlay benchmark")
        (println (select-keys config [:row-count :col-count :entries-per-row :block-dim :reps]))
        (println (format "%-8s %-13s %9s %10s %10s %10s %12s %s"
                         "scenario" "selection"
                         "entries" "median ms"
                         "p95 ms" "p99 ms"
                         "alloc bytes" "<50ms"))
        {:replacements
         (run-overlay-scenario mapped config :replacements-100k (replacement-ledger config 100000))
         :structural (run-overlay-scenario mapped
                                           config
                                           :structural-1000-rows
                                           (structural-ledger config 1000))})
      (finally (Files/deleteIfExists path)))))

(def ^:private api-lookup-count 1000000)

(def ^:private api-warmup-ns 1000000000)

(def ^:private api-samples 40)

(def ^:private api-page-capacity 4096)

(defn- lookup-coordinates
  "Fixed-seed point coordinates; about half of them miss."
  [{:keys [row-count col-count entries-per-row]}]
  (let [random
        (SplittableRandom. 20260927)

        stored-cols
        (long entries-per-row)

        missing-cols
        (- (long col-count) stored-cols)

        rows
        (long-array api-lookup-count)

        cols
        (long-array api-lookup-count)]

    (dotimes [index api-lookup-count]
      (aset rows index (.nextLong random (long row-count)))
      (aset cols
            index
            (if (.nextBoolean random)
              (.nextLong random stored-cols)
              (+ stored-cols (.nextLong random missing-cols)))))
    {:rows rows :cols cols}))

(defn- expected-find-entry-sum
  ^long [{:keys [entries-per-row]} ^longs rows ^longs cols]
  (let [stored-cols (long entries-per-row)]
    (loop [index 0
           sum 0]

      (if (< index (alength rows))
        (let [col (aget cols index)]
          (recur (inc index)
                 (+ sum (if (< col stored-cols) (+ (* (aget rows index) stored-cols) col) -1))))
        sum))))

(defn- expected-hits
  ^long [{:keys [entries-per-row]} ^longs cols]
  (let [stored-cols (long entries-per-row)]
    (loop [index 0
           hits 0]

      (if (< index (alength cols))
        (recur (inc index) (if (< (aget cols index) stored-cols) (inc hits) hits))
        hits))))

(defn- find-entry-sum
  ^long [mapped ^longs rows ^longs cols]
  (loop [index
         0

         sum
         0]

    (if (< index (alength rows))
      (recur (inc index) (+ sum (csr64/find-entry mapped (aget rows index) (aget cols index))))
      sum)))

(defn- copy-point-hits
  ^long [mapped ^longs rows ^longs cols ^doubles dst]
  (loop [index
         0

         hits
         0]

    (if (< index (alength rows))
      (recur
        (inc index)
        (if (csr64/copy-point! mapped (aget rows index) (aget cols index) dst 0) (inc hits) hits))
      hits)))

(defn- open-entry-count
  ^long [path]
  (with-open [source (csr64/open-artifact path)]
    (csr64/range-entry-count source 0 (:row-count (csr64/metadata source)))))

(defn- expected-drain-sum
  "Sum of row id plus column id over every entry of the uniform layout."
  ^long [{:keys [row-count entries-per-row]}]
  (let [rows
        (long row-count)

        per-row
        (long entries-per-row)]

    (+ (quot (* per-row rows (dec rows)) 2) (quot (* rows per-row (dec per-row)) 2))))

(defn- page-id-sum
  ^long [^ints rows ^ints cols ^long entry-count]
  (loop [index
         0

         sum
         0]

    (if (< index entry-count) (recur (inc index) (+ sum (aget rows index) (aget cols index))) sum)))

(defn- drain-copy-page-sum
  "Drains every row with a hand-written `copy-page!` cursor loop."
  ^long [mapped ^ints rows ^ints cols ^doubles values]
  (let [row-end
        (csr64/row-count mapped)

        total
        (csr64/entry-count mapped)

        capacity
        (alength rows)]

    (loop [cursor
           0

           sum
           0]

      (if (< cursor total)
        (let [copied (long (csr64/copy-page! mapped 0 row-end cursor capacity rows cols values 0))]
          (recur (+ cursor copied) (+ sum (page-id-sum rows cols copied))))
        sum))))

(defn- drain-pages-sum
  "Drains every row by reducing over `csr64/pages`."
  ^long [mapped buf]
  (reduce (fn [^long sum page]
            (+ sum (page-id-sum (csr64/page-rows page) (csr64/page-cols page) (count page))))
          0
          (csr64/pages mapped buf)))

(defn- page-lane-0-sum
  ^double [^doubles values ^long dim ^long entry-count]
  (loop [index
         0

         sum
         0.0]

    (if (< index entry-count) (recur (inc index) (+ sum (aget values (* index dim)))) sum)))

(defn- lane-0-copy-pages-sum
  "Sums lane 0 of every entry by copying pages with `copy-page!`, then reading the copies."
  ^double [mapped ^ints rows ^ints cols ^doubles values]
  (let [row-end
        (csr64/row-count mapped)

        total
        (csr64/entry-count mapped)

        dim
        (csr64/block-dim mapped)

        capacity
        (alength rows)]

    (loop [cursor
           0

           sum
           0.0]

      (if (< cursor total)
        (let [copied (long (csr64/copy-page! mapped 0 row-end cursor capacity rows cols values 0))]
          (recur (+ cursor copied) (+ sum (page-lane-0-sum values dim copied))))
        sum))))

(defn- lane-0-reduce-entries-sum
  "Sums lane 0 of every entry straight from the mapping with `csr64/reduce-entries`."
  ^double [mapped]
  (csr64/reduce-entries [_row _col e] [mapped] [acc 0.0] (+ acc (csr64/lane mapped e 0))))

(defn- api-variants
  [mapped path config]
  (let [{:keys [rows cols]}
        (lookup-coordinates config)

        dst
        (double-array (:block-dim config))

        entry-count
        (* (long (:row-count config)) (long (:entries-per-row config)))

        drain-sum
        (expected-drain-sum config)

        page-rows
        (int-array api-page-capacity)

        page-cols
        (int-array api-page-capacity)

        page-values
        (double-array (* api-page-capacity (long (:block-dim config))))]

    [{:label :find-entry
      :f #(find-entry-sum mapped rows cols)
      :expected (expected-find-entry-sum config rows cols)}
     {:label :copy-point
      :f #(copy-point-hits mapped rows cols dst)
      :expected (expected-hits config cols)}
     {:label :open-and-validate :f #(open-entry-count path) :expected entry-count}
     {:label :drain-copy-page
      :f #(drain-copy-page-sum mapped page-rows page-cols page-values)
      :expected drain-sum}
     (let [buf (csr64/page mapped api-page-capacity)]
       {:label :drain-pages :f #(drain-pages-sum mapped buf) :expected drain-sum})
     {:label :lane-0-copy-pages
      :f #(lane-0-copy-pages-sum mapped page-rows page-cols page-values)
      :expected (double entry-count)}
     {:label :lane-0-reduce-entries
      :f #(lane-0-reduce-entries-sum mapped)
      :expected (double entry-count)}]))

(defn- check-api-result!
  [label expected actual]
  (when-not (= expected actual)
    (throw (ex-info "CSR64 API benchmark result mismatch."
                    {:variant label :expected expected :actual actual}))))

(defn- sample-variants
  "Warms every variant, then samples them in rotating order and checks every result."
  [variants]
  (doseq [{:keys [label f expected]} variants]
    (let [deadline (+ (System/nanoTime) (long api-warmup-ns))]
      (while (< (System/nanoTime) deadline) (check-api-result! label expected (f)))))
  (let [variant-count
        (count variants)

        times
        (vec (repeatedly variant-count #(long-array api-samples)))

        allocations
        (vec (repeatedly variant-count #(long-array api-samples)))]

    (dotimes [sample api-samples]
      (dotimes [offset variant-count]
        (let [index (mod (+ sample offset) variant-count)
              {:keys [label f expected]} (nth variants index)
              before-bytes (allocated-bytes)
              start (System/nanoTime)
              actual (f)
              elapsed (- (System/nanoTime) start)
              allocated (- (allocated-bytes) before-bytes)]

          (check-api-result! label expected actual)
          (aset ^longs (nth times index) sample elapsed)
          (aset ^longs (nth allocations index) sample allocated))))
    (mapv (fn [variant ^longs variant-times ^longs variant-allocations]
            (Arrays/sort variant-times)
            (Arrays/sort variant-allocations)
            (-> (dissoc variant :f)
                (assoc :median-ns (aget variant-times (quot api-samples 2))
                       :median-allocated-bytes (aget variant-allocations (quot api-samples 2)))))
          variants
          times
          allocations)))

(defn- print-api-results
  [results]
  (println (format "%-22s %10s %16s" "variant" "median ms" "alloc bytes/call"))
  (doseq [{:keys [label median-ns median-allocated-bytes]} results]
    (println (format "%-22s %10.3f %,16d"
                     (name label)
                     (/ (long median-ns) 1000000.0)
                     median-allocated-bytes))))

(defn run-api-config
  "Measures the Clojure-facing CSR64 API against its low-level equivalents in one JVM."
  [config]
  (let [path (temporary-artifact)]
    (try (csr64/write-artifact! (uniform-source config) path {:page-entries 16320})
         (with-open [mapped (csr64/open-artifact path)]
           (csr64/load! mapped)
           (let [results (sample-variants (api-variants mapped path config))]
             (println)
             (println (str "CSR64 API / " (:label config)))
             (println {:java (System/getProperty "java.runtime.version")
                       :rows (:row-count config)
                       :entries (* (long (:row-count config)) (long (:entries-per-row config)))
                       :block-dim (:block-dim config)
                       :lookups api-lookup-count
                       :warmup-ms-per-variant (quot (long api-warmup-ns) 1000000)
                       :samples api-samples
                       :loaded-hint (csr64/loaded? mapped)})
             (print-api-results results)
             results))
         (finally (Files/deleteIfExists path)))))

(defn- drained-output
  "Drains a row range through `csr64/pages` into a fresh destination."
  [mapped row-start row-end capacity]
  (let [dim
        (csr64/block-dim mapped)

        output
        (destination (csr64/range-entry-count mapped row-start row-end) dim)]

    (reduce (fn [^long offset page]
              (let [copied (count page)]
                (System/arraycopy (csr64/page-rows page) 0 (:rows output) offset copied)
                (System/arraycopy (csr64/page-cols page) 0 (:cols output) offset copied)
                (System/arraycopy (csr64/page-values page)
                                  0
                                  (:values output)
                                  (* offset dim)
                                  (* copied dim))
                (+ offset copied)))
            0
            (csr64/pages mapped row-start row-end (csr64/page mapped capacity)))
    output))

(defn smoke-check
  []
  (let [config
        (:smoke configs)

        generated
        (uniform-source config)

        heap
        (heap-source config)

        path
        (temporary-artifact)]

    (try (csr64/write-artifact! generated path {:page-entries 64})
         (with-open [mapped (csr64/open-artifact path)]
           (let [entry-count (* 8 (:entries-per-row config))
                 expected (destination entry-count block-dim)
                 actual (destination entry-count block-dim)
                 replacement (double-array block-dim)]

             (assert (= entry-count ((runner :heap heap 4 12 entry-count expected))))
             (assert (= entry-count ((runner :csr64 mapped 4 12 entry-count actual))))
             (assert (same-output? expected actual))
             (assert (same-output? expected (drained-output mapped 4 12 5)))
             (assert
               (= (mapv vector (:rows expected) (:cols expected))
                  (csr64/reduce-entries [row col _e] [mapped 4 12] [acc []] (conj acc [row col]))))
             (Arrays/fill replacement 2.0)
             (let [view (overlay/compile-ledger
                          mapped
                          [{:sequence 0 :op :put :row-id 4 :col-id 0 :block replacement}])]
               (assert (= entry-count
                          (overlay/copy-page! view
                                              4
                                              12
                                              0
                                              entry-count
                                              (:rows actual)
                                              (:cols actual)
                                              (:values actual)
                                              0)))
               (assert (= 2.0 (aget ^doubles (:values actual) 0)))
               (assert (= 1.0 (aget ^doubles (:values actual) block-dim))))))
         (println "CSR64 benchmark smoke check passed")
         (finally (Files/deleteIfExists path)))))

(defn -main
  [& args]
  (cond (some #{"smoke"} args) (smoke-check)
        (some #{"overlay"} args) (run-overlay-config (:medium configs))
        (some #{"api"} args) (run-api-config (:medium configs))
        :else (run-config (if (some #{"ceiling"} args) (:ceiling configs) (:medium configs)))))
