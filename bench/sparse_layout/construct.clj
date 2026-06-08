(ns sparse-layout.construct
  "Isolated CSR/CSC construction benchmark.

  Ingests COO once per config, then measures only the shipped freeze step
  (`freeze-builder!`). Reports wall-clock mean and per-call heap allocation."
  (:gen-class)
  (:require [sparse-layout.core :as sparse]
            [sparse-layout.bench-data :as data])
  (:import [java.lang.management ManagementFactory]
           [com.sun.management ThreadMXBean]))

(defn- ingest-builder
  "Builds a mutable COO builder from flat entries without freezing. `freeze!`
  does not mutate the builder, so the same builder can be frozen repeatedly."
  [entries]
  (let [builder (data/make-bench-features-builder)]
    (doseq [[row col payload] entries]
      (sparse/append-entry! builder row col payload))
    builder))

(def ^:private ^ThreadMXBean thread-mx-bean
  (let [bean (ManagementFactory/getThreadMXBean)]
    (cast ThreadMXBean bean)))

(defn- allocated-bytes ^long []
  (.getThreadAllocatedBytes thread-mx-bean (.getId (Thread/currentThread))))

(defn- measure-allocation
  "Mean bytes allocated per call to `f`, averaged over `reps` after a warmup."
  ^long [f reps]
  (dotimes [_ 5] (f))
  (let [before (allocated-bytes)]
    (dotimes [_ reps] (f))
    (quot (- (allocated-bytes) before) reps)))

(defn- mean-ns
  "Mean wall-clock nanoseconds per call to `f`, measured after an explicit
  warmup so steady-state (JIT-compiled) cost is separated from cold start."
  ^double [f reps]
  (dotimes [_ (max 200 reps)] (f))
  (let [reps (long reps)
        start (System/nanoTime)]
    (dotimes [_ reps] (f))
    (/ (double (- (System/nanoTime) start)) reps)))

(defn- format-bytes [bytes]
  (let [units ["B" "KiB" "MiB" "GiB"]
        bytes (double bytes)]
    (loop [value bytes
           unit 0]
      (if (or (< value 1024.0) (= unit (dec (count units))))
        (format "%.2f %s" value (nth units unit))
        (recur (/ value 1024.0) (inc unit))))))

(defn- format-us [ns]
  (format "%.1f µs" (/ (double ns) 1000.0)))

(defn- run-config [config reps]
  (data/print-config config)
  (let [entries (data/make-entries config)
        builder (ingest-builder entries)
        freeze! #(sparse/freeze-builder! builder)
        elapsed-ns (mean-ns freeze! reps)
        allocated-bytes (measure-allocation freeze! reps)]
    (println)
    (println (format "%-26s %16s" "metric" "freeze"))
    (println (format "%-26s %16s"
                     "freeze mean time"
                     (format-us elapsed-ns)))
    (println (format "%-26s %16s"
                     "freeze alloc / call"
                     (format-bytes allocated-bytes)))
    (println (format "%-26s %16.1f"
                     "freeze alloc B/nnz"
                     (/ (double allocated-bytes) (data/nnz config))))))

(defn run
  ([] (run (data/standard-configs)))
  ([configs]
   (println "sparse-layout construction benchmark")
   (println "Isolated freeze: shipped COO -> CSR/CSC compilation path.")
   (doseq [config configs]
     (run-config config 50))))

(defn -main [& args]
  (run (data/selected-configs args)))
