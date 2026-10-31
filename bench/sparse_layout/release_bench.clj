(ns sparse-layout.release-bench
  "Dependency-free core release comparison; input construction and checking are untimed."
  (:require [clojure.edn :as edn]
            [clojure.java.shell :as shell]
            [sparse-layout.core :as sparse :refer [defsparse]])
  (:import [com.sun.management ThreadMXBean]
           [java.lang.management ManagementFactory]
           [java.security MessageDigest]
           [java.util HexFormat]))

(defsparse release-blocks
           {:row-key [:row]
            :cols-path [:values]
            :payload {:kind :fixed-double-block :dim 8}
            :indices #{:csr :csc}})

(set! *warn-on-reflection* true)

(def ^:private sink (volatile! nil))

(defn expected-block [row col] (mapv #(+ (* row 0.5) (* col 0.125) (* % 0.03125)) (range 8)))

(defn ingest
  "Input tuples and double arrays already exist; builder copies are timed."
  [entries]
  (let [builder (make-release-blocks-builder)]
    (doseq [[row col block] entries]
      (sparse/append-entry! builder row col block))
    builder))

(defn fixture
  [smoke?]
  (let [rows
        (if smoke? 4 256)

        degree
        (if smoke? 4 32)

        entries
        (vec (for [row
                   (range rows)

                   col
                   (range degree)]

               [row col (double-array (expected-block row col))]))

        builder
        (ingest entries)]

    {:rows rows
     :degree degree
     :dim 8
     :entries entries
     :builder builder
     :dataset (release-blocks-freeze! builder)}))

(defn check-equal!
  [label expected actual]
  (when-not (= expected actual)
    (throw (ex-info "Release benchmark oracle mismatch."
                    {:case label :expected expected :actual actual})))
  true)

(defn- materialize
  [blocks view?]
  (mapv (fn [[key block]]
          [key (if view? (sparse/block-view->vec block) (vec block))])
        blocks))

(defn verify-dataset!
  [{:keys [rows degree]} dataset]
  (doseq [row (range rows)]
    (let [expected (mapv (fn [col]
                           [col (expected-block row col)])
                         (range degree))]
      (check-equal! :row-copy expected (materialize (release-blocks-row dataset row) false))
      (check-equal! :row-view expected (materialize (release-blocks-row-views dataset row) true))))
  (doseq [col (range degree)]
    (check-equal! :col-view
                  (mapv (fn [row]
                          [row (expected-block row col)])
                        (range rows))
                  (materialize (release-blocks-col-views dataset col) true)))
  (doseq [row
          (range rows)

          col
          (range degree)]

    (check-equal! :point (expected-block row col) (vec (release-blocks-block dataset row col))))
  (check-equal! :missing nil (release-blocks-block dataset rows degree))
  true)

(defn cases
  [{:keys [rows degree dataset entries builder] :as fixture}]
  (let [row
        (quot rows 2)

        col
        (quot degree 2)

        row-expected
        (mapv (fn [c]
                [c (expected-block row c)])
              (range degree))

        col-expected
        (mapv (fn [r]
                [r (expected-block r col)])
              (range rows))]

    [{:id :ingest-freeze
      :batch 1
      :f #(release-blocks-freeze! (ingest entries))
      :check #(verify-dataset! fixture %)}
     {:id :freeze :batch 1 :f #(release-blocks-freeze! builder) :check #(verify-dataset! fixture %)}
     {:id :point-copy
      :batch 1024
      :f #(release-blocks-block dataset row col)
      :check #(check-equal! :point (expected-block row col) (vec %))}
     {:id :row-copy
      :batch 64
      :f #(release-blocks-row dataset row)
      :check #(check-equal! :row-copy row-expected (materialize % false))}
     {:id :row-view
      :batch 64
      :f #(release-blocks-row-views dataset row)
      :check #(check-equal! :row-view row-expected (materialize % true))}
     {:id :col-view
      :batch 16
      :f #(release-blocks-col-views dataset col)
      :check #(check-equal! :col-view col-expected (materialize % true))}]))

(def ^:private ^ThreadMXBean allocation-bean
  (let [bean (ManagementFactory/getThreadMXBean)]
    (when (and (instance? ThreadMXBean bean) (.isThreadAllocatedMemorySupported ^ThreadMXBean bean))
      (.setThreadAllocatedMemoryEnabled ^ThreadMXBean bean true)
      bean)))

(defn- allocated
  ^long []
  (if allocation-bean
    (.getThreadAllocatedBytes allocation-bean (.getId (Thread/currentThread)))
    -1))

(defn- invoke-batch!
  [{:keys [f batch]}]
  (dotimes [_ batch]
    (vreset! sink (f))))

(defn- sample
  [case]
  (let [before
        (allocated)

        start
        (System/nanoTime)]

    (invoke-batch! case)
    (let [elapsed
          (- (System/nanoTime) start)

          after
          (allocated)

          batch
          (:batch case)]

      {:ns (/ (double elapsed) batch)
       :allocated-bytes (when (not= -1 before) (/ (double (- after before)) batch))})))

(defn- percentile
  [values fraction]
  (nth (vec (sort values)) (max 0 (dec (long (Math/ceil (* fraction (count values))))))))

(defn- summarize
  [observations]
  (let [times
        (mapv :ns observations)

        allocations
        (keep :allocated-bytes observations)]

    {:median-ns (percentile times 0.5)
     :p95-ns (percentile times 0.95)
     :median-allocated-bytes (when (seq allocations) (percentile allocations 0.5))
     :p95-allocated-bytes (when (seq allocations) (percentile allocations 0.95))
     :samples observations}))

(defn- measure
  [cases warmup-ms samples]
  (let [observations (mapv (fn [_]
                             (atom []))
                           cases)]
    (doseq [case cases]
      (let [deadline (+ (System/nanoTime) (* 1000000 (long warmup-ms)))]
        (loop []

          (invoke-batch! case)
          (when (< (System/nanoTime) deadline) (recur)))))
    (dotimes [round samples]
      (dotimes [offset (count cases)]
        (let [i (mod (+ round offset) (count cases))]
          (swap! (nth observations i) conj (sample (nth cases i))))))
    (mapv (fn [case observations]
            (merge (select-keys case [:id :batch]) (summarize @observations)))
          cases
          observations)))

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
   :core-sha256 (.formatHex (HexFormat/of)
                            (.digest (MessageDigest/getInstance "SHA-256")
                                     (.getBytes (slurp "src/sparse_layout/core.clj") "UTF-8")))})

(defn run-benchmark
  [options]
  (let [{:keys [smoke? check-only? samples warmup-ms] :as options}
        (merge {:smoke? false :check-only? false :samples 31 :warmup-ms 1000} options)]
    (when-not (and (pos-int? samples)
                   (nat-int? warmup-ms)
                   (or smoke? check-only? (and (>= samples 31) (>= warmup-ms 1000))))
      (throw (ex-info "Require >=31 samples and >=1000ms warmup outside smoke/check mode."
                      options)))
    (let [fixture (fixture smoke?)
          cases (cases fixture)
          cases (if smoke? (mapv #(assoc % :batch 1) cases) cases)]

      (verify-dataset! fixture (:dataset fixture))
      (doseq [{:keys [f check]} cases]
        (check (f)))
      (let [results (when-not check-only? (measure cases warmup-ms samples))]
        (doseq [{:keys [f check]} cases]
          (check (f)))
        {:environment (environment)
         :options options
         :oracle :passed
         :fixture (assoc (select-keys fixture [:rows :degree :dim])
                    :entries (count (:entries fixture))
                    :indices [:csr :csc]
                    :query-row (quot (:rows fixture) 2)
                    :query-col (quot (:degree fixture) 2))
         :semantics {:inputs "prebuilt row/column tuples and double arrays; builder copies included"
                     :freeze "repeatable freeze of same pre-ingested builder; setup excluded"
                     :queries "fixed middle hit, whole eager row/column; no payload sum timed"
                     :consumption "every result escapes via volatile sink; sink overhead included"
                     :allocation "current-thread heap bytes/op, including returned arrays/views"
                     :validation "all coordinates and lanes checked outside measurement"}
         :cases results}))))

(defn -main
  [& args]
  (let [options (case (first args)
                  nil
                  {}

                  "--smoke"
                  {:smoke? true :warmup-ms 1 :samples 3}

                  "--check"
                  {:check-only? true}

                  (edn/read-string (first args)))]
    (try (prn (run-benchmark options)) (finally (shutdown-agents)))))
