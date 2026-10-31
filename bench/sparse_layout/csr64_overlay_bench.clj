(ns sparse-layout.csr64-overlay-bench
  "Oracle-checked bounded overlay copies. Timings exclude fixture I/O and validation."
  (:require [clojure.edn :as edn]
            [clojure.java.shell :as shell]
            [sparse-layout.csr-source :as csr]
            [sparse-layout.csr64 :as csr64]
            [sparse-layout.csr64-overlay :as overlay])
  (:import [com.sun.management ThreadMXBean]
           [java.lang.management ManagementFactory]
           [java.nio.file Files]
           [java.security MessageDigest]
           [java.util Arrays HexFormat]))

(set! *warn-on-reflection* true)

(def ^:private sentinel -987654321)
(def ^:private sink (volatile! 0.0))

(defn fixture-value
  ^double [^long row ^long col ^long lane]
  (+ (* row 0.5) (* col 0.125) (* lane 0.03125)))

(defn fixture-source
  "Even columns leave gaps for insertions; each payload lane is distinguishable."
  [rows degree dim]
  (let [entries
        (* rows degree)

        cols
        (int-array entries)

        values
        (double-array (* entries dim))]

    (dotimes [row rows]
      (dotimes [j degree]
        (let [entry (+ (* row degree) j)
              col (* 2 j)]

          (aset cols entry (int col))
          (dotimes [lane dim]
            (aset values (+ (* entry dim) lane) (fixture-value row col lane))))))
    (csr/->HeapCSRSource {}
                         (object-array rows)
                         {}
                         (object-array (inc (* 2 degree)))
                         (int-array (map #(* degree %) (range (inc rows))))
                         cols
                         values
                         dim
                         nil
                         nil)))

(defn ledger
  "Tail inserts, or inserts/deletes/repeated replacements, on every row."
  [rows degree dim kind]
  (vec
    (map-indexed (fn [sequence record]
                   (assoc record :sequence sequence))
                 (mapcat
                   (fn [row]
                     (let [put
                           (fn [col value]
                             {:op :put
                              :row-id row
                              :col-id col
                              :block (double-array (map #(+ value (* % 0.0625)) (range dim)))})

                           delete
                           (fn [col]
                             {:op :delete :row-id row :col-id col})]

                       (case kind
                         :clean
                         []

                         :tail
                         [(put (* 2 degree) -10.0)]

                         :mixed
                         [(put (* 2 degree) -10.0) (delete 0) (put 1 -20.0)
                          (delete (* 2 (quot degree 2))) (put (inc (* 2 (quot degree 2))) -30.0)
                          (put 2 -40.0) (put 2 -41.0)])))
                   (range rows)))))

(defn oracle
  "Independent ordered-map replay, without querying the compiled overlay."
  [rows degree dim modifications]
  (vec
    (reduce (fn [entries {:keys [op row-id col-id block]}]
              (case op
                :delete
                (dissoc entries [row-id col-id])

                :put
                (assoc entries [row-id col-id] (vec block))))
            (into (sorted-map)
                  (for [row
                        (range rows)

                        j
                        (range degree)

                        :let [col
                              (* 2 j)]]

                    [[row col] (mapv #(fixture-value row col %) (range dim))]))
            modifications)))

(defn destination
  [capacity dim]
  (let [rows
        (int-array (+ capacity 4))

        cols
        (int-array (+ capacity 4))

        values
        (double-array (* (+ capacity 4) dim))]

    (Arrays/fill rows (int sentinel))
    (Arrays/fill cols (int sentinel))
    (Arrays/fill values (double sentinel))
    {:rows rows :cols cols :values values :offset 2 :capacity capacity :dim dim}))

(defn check-page!
  "Checks every output cell and both untouched sentinel regions, bit-exactly."
  [expected cursor copied {:keys [rows cols values offset capacity dim]}]
  (let [rows
        ^ints rows

        cols
        ^ints cols

        values
        ^doubles values

        n
        (min capacity (- (count expected) cursor))]

    (when-not (= n copied)
      (throw (ex-info "Wrong copied count." {:expected n :actual copied :cursor cursor})))
    (dotimes [i (alength rows)]
      (let [inside? (<= offset i (dec (+ offset n)))
            [[row col] block] (when inside? (nth expected (+ cursor (- i offset))))
            expected-row (if inside? row sentinel)
            expected-col (if inside? col sentinel)]

        (when-not (and (= expected-row (aget rows i)) (= expected-col (aget cols i)))
          (throw (ex-info "Coordinate or sentinel mismatch." {:index i :cursor cursor})))
        (dotimes [lane dim]
          (let [value (if inside? (nth block lane) (double sentinel))]
            (when-not (= (Double/doubleToRawLongBits (double value))
                         (Double/doubleToRawLongBits (aget values (+ (* i dim) lane))))
              (throw (ex-info "Payload or sentinel mismatch."
                              {:index i :lane lane :cursor cursor})))))))
    true))

(defn- copy!
  [compiled rows cursor {:keys [rows-out cols-out values-out offset capacity]}]
  (overlay/copy-page! compiled 0 rows cursor capacity rows-out cols-out values-out offset))

(defn- copy-target
  [dst]
  (assoc dst
    :rows-out (:rows dst)
    :cols-out (:cols dst)
    :values-out (:values dst)))

(defn verify-case!
  "Fresh sentinels per page ensure stale data cannot mask short/partial writes."
  [{:keys [compiled rows dim expected cursors capacity]}]
  (doseq [cursor cursors]
    (let [dst (destination capacity dim)
          copied (copy! compiled rows cursor (copy-target dst))]

      (check-page! expected cursor copied dst)))
  true)

(defn- consume!
  [dst copied]
  (let [values
        ^doubles (:values dst)

        cols
        ^ints (:cols dst)

        offset
        (long (:offset dst))

        dim
        (long (:dim dst))]

    (when (pos? (long copied))
      (vreset! sink
               (+ (aget values (* offset dim))
                  (aget values (dec (* (+ offset (long copied)) dim)))
                  (aget cols (+ offset (dec (long copied)))))))))

(defn- operation
  [{:keys [compiled rows dim cursors capacity]}]
  (let [dst
        (destination capacity dim)

        target
        (copy-target dst)]

    (fn []
      (doseq [cursor cursors]
        (consume! dst (copy! compiled rows cursor target))))))

(def ^:private ^ThreadMXBean allocation-bean
  (let [bean (ManagementFactory/getThreadMXBean)]
    (when (and (instance? ThreadMXBean bean) (.isThreadAllocatedMemorySupported ^ThreadMXBean bean))
      (.setThreadAllocatedMemoryEnabled ^ThreadMXBean bean true)
      bean)))

(defn- allocated
  ^long []
  (if allocation-bean
    (.getThreadAllocatedBytes allocation-bean (.threadId (Thread/currentThread)))
    -1))

(defn- sample
  [f batch]
  (let [before
        (allocated)

        start
        (System/nanoTime)]

    (dotimes [_ batch]
      (f))
    (let [elapsed
          (- (System/nanoTime) start)

          bytes
          (allocated)]

      {:ns (/ (double elapsed) batch)
       :allocated-bytes (when (not= -1 before) (/ (double (- bytes before)) batch))})))

(defn- percentile
  [values fraction]
  (nth (vec (sort values)) (max 0 (dec (long (Math/ceil (* fraction (count values))))))))

(defn- summarize
  [samples]
  (let [times
        (mapv :ns samples)

        bytes
        (keep :allocated-bytes samples)]

    {:median-ns (percentile times 0.5)
     :p95-ns (percentile times 0.95)
     :median-allocated-bytes (when (seq bytes) (percentile bytes 0.5))
     :p95-allocated-bytes (when (seq bytes) (percentile bytes 0.95))
     :samples samples}))

(defn measure-cases
  "Sustained per-case warmup; subsequent sample rounds rotate the first case."
  [cases {:keys [warmup-ms samples batch]}]
  (let [operations
        (mapv operation cases)

        observations
        (mapv (fn [_]
                (atom []))
              cases)]

    (doseq [f operations]
      (let [deadline (+ (System/nanoTime) (* 1000000 (long warmup-ms)))]
        (loop []

          (dotimes [_ batch]
            (f))
          (when (< (System/nanoTime) deadline) (recur)))))
    (dotimes [round samples]
      (dotimes [offset (count cases)]
        (let [i (mod (+ round offset) (count cases))]
          (swap! (nth observations i) conj (sample (nth operations i) batch)))))
    (mapv (fn [case observations]
            (merge (select-keys case
                                [:id :rows :degree :dim :kind :capacity :cursors :logical-entries
                                 :logical-payload-bytes])
                   (summarize @observations)))
          cases
          observations)))

(defn configurations
  [smoke?]
  (let [wide
        (if smoke? 32 100000)

        drain
        (if smoke? 32 4096)]

    [{:id :wide-tail :rows 1 :degree wide :dim 8 :kind :tail :pages? true}
     {:id :wide-mixed :rows 1 :degree wide :dim 8 :kind :mixed :pages? true}
     {:id :bounded-drain :rows 1 :degree drain :dim 8 :kind :mixed}
     {:id :short-rows :rows (if smoke? 4 1024) :degree 16 :dim 8 :kind :mixed}
     {:id :clean-copy :rows 1 :degree drain :dim 8 :kind :clean :full? true}]))

(defn- make-cases
  [config compiled expected page-size]
  (let [total
        (count expected)

        capacity
        (if (:full? config) total page-size)

        base
        (assoc config
          :compiled compiled
          :expected expected
          :capacity capacity)

        positions
        (if (:pages? config)
          [[:early 0] [:middle (quot total 2)] [:late (max 0 (- total page-size))]]
          [[:all nil]])]

    (mapv (fn [[position cursor]]
            (let [cursors
                  (if (some? cursor) [cursor] (vec (range 0 total capacity)))

                  entries
                  (reduce + (map #(min capacity (- total %)) cursors))]

              (assoc base
                :id (keyword (str (name (:id config)) "-" (name position)))
                :cursors cursors
                :logical-entries entries
                :logical-payload-bytes (* entries (:dim config) Double/BYTES))))
          positions)))

(defn with-cases
  "Owns mapped sources and temporary artifacts for the callback's lifetime."
  [smoke? f]
  (let [directory
        (Files/createTempDirectory "csr64-overlay-bench-"
                                   (make-array java.nio.file.attribute.FileAttribute 0))

        resources
        (atom [])]

    (try
      (let [groups (mapv (fn [{:keys [id rows degree dim kind] :as config}]
                           (let [path (.resolve directory (str (name id) ".csr64"))]
                             (swap! resources conj {:path path})
                             (csr64/write-artifact! (fixture-source rows degree dim) path)
                             (let [source (csr64/open-artifact path)
                                   _ (swap! resources conj {:source source})
                                   _ (csr64/load! source)
                                   modifications (ledger rows degree dim kind)
                                   expected (oracle rows degree dim modifications)
                                   before (allocated)
                                   start (System/nanoTime)
                                   compiled (overlay/compile-ledger source modifications)
                                   elapsed (- (System/nanoTime) start)
                                   bytes (allocated)]

                               {:compile {:id id
                                          :ns elapsed
                                          :allocated-bytes (when (not= -1 before) (- bytes before))
                                          :ledger-records (count modifications)}
                                :cases (make-cases config compiled expected (if smoke? 7 64))})))
                         (configurations smoke?))]
        (f (vec (mapcat :cases groups)) (mapv :compile groups)))
      (finally (doseq [{:keys [source]}
                       (reverse @resources)

                       :when source]

                 (.close ^java.io.Closeable source))
               (doseq [{:keys [path]}
                       @resources

                       :when path]

                 (Files/deleteIfExists path))
               (Files/deleteIfExists directory)))))

(defn- environment
  []
  {:java-version (System/getProperty "java.version")
   :vm (System/getProperty "java.vm.name")
   :clojure-version (clojure-version)
   :os (System/getProperty "os.name")
   :arch (System/getProperty "os.arch")
   :processors (.availableProcessors (Runtime/getRuntime))
   :jvm-arguments (vec (.getInputArguments (ManagementFactory/getRuntimeMXBean)))
   :git-head (.trim ^String (:out (shell/sh "git" "rev-parse" "HEAD")))
   :overlay-sha256 (.formatHex (HexFormat/of)
                               (.digest (MessageDigest/getInstance "SHA-256")
                                        (.getBytes (slurp "src/sparse_layout/csr64_overlay.clj")
                                                   "UTF-8")))})

(defn run-benchmark
  [options]
  (let [{:keys [smoke? check-only? warmup-ms samples batch] :as options}
        (merge {:smoke? false :check-only? false :warmup-ms 1000 :samples 31 :batch 1} options)]
    (when-not (and (pos-int? samples)
                   (pos-int? batch)
                   (nat-int? warmup-ms)
                   (or smoke? check-only? (and (>= warmup-ms 1000) (>= samples 31))))
      (throw (ex-info "Use >=1000ms warmup and >=31 samples outside smoke/check mode." options)))
    (with-cases smoke?
                (fn [cases compile-results]
                  (doseq [case cases]
                    (verify-case! case))
                  (let [results (when-not check-only? (measure-cases cases options))]
                    (doseq [case cases]
                      (verify-case! case))
                    {:environment (environment)
                     :options options
                     :oracle :passed
                     :semantics {:read "copy-page! into reused primitive arrays at entry offset 2"
                                 :consumption "first/last payload lanes and final column per page"
                                 :payload "logical double payload bytes; not memory-bus traffic"
                                 :compile "single cold compilation per fixture, separate from reads"
                                 :allocation
                                 "current-thread allocated heap bytes; excludes native mappings"}
                     :compile compile-results
                     :cases results
                     :sink @sink})))))

(defn dense-ledger
  "Replace alternating existing entries, plus a tail insert: one-entry clean runs."
  [degree dim]
  (mapv (fn [sequence col]
          {:sequence sequence
           :op :put
           :row-id 0
           :col-id col
           :block (double-array (map #(- (+ 100.0 col (* % 0.0625))) (range dim)))})
        (range)
        (concat (range 0 (* 2 degree) 4) [(* 2 degree)])))

(defn with-cost-cases
  "Cost fixtures own their resources; fixture construction is outside timed operations."
  [smoke? f]
  (let [directory
        (Files/createTempDirectory "csr64-overlay-cost-"
                                   (make-array java.nio.file.attribute.FileAttribute 0))

        resources
        (atom [])]

    (try
      (let [fixtures
            (mapv (fn [[id degree dense?]]
                    (let [path (.resolve directory (str (name id) ".csr64"))
                          dim 8]

                      (swap! resources conj {:path path})
                      (csr64/write-artifact! (fixture-source 1 degree dim) path)
                      (let [source (csr64/open-artifact path)
                            _ (swap! resources conj {:source source})
                            _ (csr64/load! source)
                            modifications
                            (if dense? (dense-ledger degree dim) (ledger 1 degree dim :tail))
                            expected (oracle 1 degree dim modifications)
                            compiled (overlay/compile-ledger source modifications)]

                        {:id id
                         :source source
                         :modifications modifications
                         :case {:id id
                                :rows 1
                                :degree degree
                                :dim dim
                                :kind (if dense? :dense :tail)
                                :capacity (count expected)
                                :cursors [0]
                                :compiled compiled
                                :expected expected
                                :logical-entries (count expected)
                                :logical-payload-bytes (* (count expected) dim Double/BYTES)}})))
                  [[:dense (if smoke? 16 4096) true] [:tail (if smoke? 32 100000) false]])]
        (f fixtures))
      (finally (doseq [{:keys [source]}
                       (reverse @resources)

                       :when source]

                 (.close ^java.io.Closeable source))
               (doseq [{:keys [path]}
                       @resources

                       :when path]

                 (Files/deleteIfExists path))
               (Files/deleteIfExists directory)))))

(defn- verify-cost-fixtures!
  [fixtures]
  (doseq [{:keys [source modifications case]} fixtures]
    (verify-case! case)
    (verify-case! (assoc case :compiled (overlay/compile-ledger source modifications))))
  true)

(defn- compilation-operation
  [{:keys [source modifications]}]
  (fn []
    ;; Metadata consumption is included in the compilation measurement.
    (vreset! sink
             (double (hash (overlay/metadata (overlay/compile-ledger source modifications)))))))

(defn- measure-cost-operations
  [operations {:keys [warmup-ms samples batch]}]
  (let [observations (mapv (fn [_]
                             (atom []))
                           operations)]
    (doseq [{:keys [f]} operations]
      (let [deadline (+ (System/nanoTime) (* 1000000 (long warmup-ms)))]
        (loop []

          (dotimes [_ batch]
            (f))
          (when (< (System/nanoTime) deadline) (recur)))))
    (dotimes [round samples]
      (dotimes [offset (count operations)]
        (let [i (mod (+ round offset) (count operations))]
          (swap! (nth observations i) conj (sample (:f (nth operations i)) batch)))))
    (mapv (fn [op observations]
            (merge (dissoc op :f) (summarize @observations)))
          operations
          observations)))

(defn run-cost-benchmark
  "Separate dense full-copy and warmed compilation costs; original read suite is unchanged."
  [options]
  (let [{:keys [smoke? check-only? warmup-ms samples batch] :as options}
        (merge {:smoke? false :check-only? false :warmup-ms 1000 :samples 31 :batch 1} options)]
    (when-not (and (pos-int? samples)
                   (pos-int? batch)
                   (nat-int? warmup-ms)
                   (or smoke? check-only? (and (>= warmup-ms 1000) (>= samples 31))))
      (throw (ex-info "Use >=1000ms warmup and >=31 samples outside smoke/check mode." options)))
    (with-cost-cases
      smoke?
      (fn [fixtures]
        (verify-cost-fixtures! fixtures)
        (let [dense (first fixtures)
              operations (into [{:id :dense-full-copy
                                 :operation :copy
                                 :degree (get-in dense [:case :degree])
                                 :dim 8
                                 :logical-entries (get-in dense [:case :logical-entries])
                                 :logical-payload-bytes (get-in dense
                                                                [:case :logical-payload-bytes])
                                 :f (operation (:case dense))}]
                               (map (fn [{:keys [id modifications case] :as fixture}]
                                      {:id (keyword (str (name id) "-compile"))
                                       :operation :compile
                                       :degree (:degree case)
                                       :dim (:dim case)
                                       :ledger-records (count modifications)
                                       :f (compilation-operation fixture)}))
                               fixtures)
              results (when-not check-only? (measure-cost-operations operations options))]

          (verify-cost-fixtures! fixtures)
          {:environment (environment)
           :mode :costs
           :options options
           :oracle :passed
           :semantics
           {:read "one full visible structural row into reused arrays at entry offset 2"
            :compile
            "warmed compile-ledger plus metadata hash consumption; defensive block copies retained"
            :payload "logical double payload bytes; not memory-bus traffic"
            :allocation "current-thread allocated heap bytes; excludes native mappings"
            :validation
            "exact ordered-map oracle before/after, including freshly compiled overlays"}
           :cases results
           :sink @sink})))))

(defn -main
  [& args]
  (if (= "--costs" (first args))
    (let [options (case (second args)
                    nil
                    {}

                    "--smoke"
                    {:smoke? true :warmup-ms 1 :samples 3}

                    "--check"
                    {:check-only? true}

                    (edn/read-string (second args)))]
      (try (prn (run-cost-benchmark options)) (finally (shutdown-agents))))
    (let [options (case (first args)
                    nil
                    {}

                    "--smoke"
                    {:smoke? true :warmup-ms 1 :samples 3}

                    "--check"
                    {:check-only? true}

                    (edn/read-string (first args)))]
      (try (prn (run-benchmark options)) (finally (shutdown-agents))))))
