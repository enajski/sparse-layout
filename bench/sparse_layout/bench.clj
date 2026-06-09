(ns sparse-layout.bench
  (:gen-class)
  (:require [criterium.core :as criterium]
            [sparse-layout.bench-data :as data]))

(defn- run-case [quick? label f]
  (println)
  (println label)
  (if quick?
    (criterium/quick-bench (f))
    (criterium/bench (f))))

(defn- run-config [quick? config]
  (data/print-config config)
  (let [{:keys [entries sparse sparse-map nested-rows nested-dual nested-vectorz row-key col-key scan-col]} (data/prepare config)]
    (println "Point lookup row/col:" [row-key col-key])
    (println "Column scan col:" scan-col)
    (run-case quick? "point lookup, generated sparse API: block-view first value"
              #(data/sparse-direct-point-value sparse row-key col-key))
    (run-case quick? "point lookup, map API: sparse facade"
              #(data/sparse-map-point-value sparse-map row-key col-key))
    (run-case quick? "point lookup, map API: nested row index (persistent vector)"
              #(data/nested-row-point-value nested-rows row-key col-key))
    (run-case quick? "point lookup, map API: nested row index (vectorz)"
              #(data/nested-vectorz-point-value nested-vectorz row-key col-key))
    (run-case quick? "row scan, row index: sparse facade reduce-kv"
              #(data/sparse-row-map-sum sparse row-key))
    (run-case quick? "row scan, row index: nested row map reduce-kv (persistent vector)"
              #(data/nested-row-map-sum nested-rows row-key))
    (run-case quick? "row scan, row index: nested row map reduce-kv (vectorz)"
              #(data/nested-vectorz-row-map-sum nested-vectorz row-key))
    (run-case quick? "column scan, column index: sparse facade reduce-kv"
              #(data/sparse-col-map-sum sparse scan-col))
    (run-case quick? "column scan, column index: nested dual index reduce-kv"
              #(data/nested-dual-col-map-sum nested-dual scan-col))
    (run-case quick? "column scan, row-only nested index: full row scan"
              #(data/nested-row-only-col-sum nested-rows scan-col))
    (run-case quick? "construction from entries: sparse COO ingest + CSR/CSC freeze"
              #(data/entries->sparse entries))
    (run-case quick? "construction from entries: nested row index (persistent vector)"
              #(data/entries->nested-row-index entries))
    (run-case quick? "construction from entries: nested row index (vectorz)"
              #(data/entries->nested-row-vectorz-index entries))
    (run-case quick? "construction from entries: nested dual row+column index"
              #(data/entries->nested-dual-index entries))))

(defn run-benchmarks
  ([] (run-benchmarks (data/standard-configs) true))
  ([configs quick?]
   (println "sparse-layout benchmarks")
   (println "Modes: default=standard quick set, large/all=all increments, full=Criterium full bench, smoke=single execution")
   (doseq [config configs]
     (run-config quick? config))))

(defn- assert-close! [label expected actual]
  (when (> (Math/abs (- (double expected) (double actual))) 1.0e-9)
    (throw (ex-info "Benchmark smoke check mismatch."
                    {:label label
                     :expected expected
                     :actual actual}))))

(defn- smoke-config [config]
  (data/print-config config)
  (let [{:keys [sparse sparse-map nested-rows nested-dual nested-vectorz row-key col-key scan-col]} (data/prepare config)
        point (data/nested-row-point-value nested-rows row-key col-key)
        row-sum (data/nested-row-map-sum nested-rows row-key)
        col-sum (data/nested-dual-col-map-sum nested-dual scan-col)
        row-only-col-sum (data/nested-row-only-col-sum nested-rows scan-col)]
    (assert-close! :point-direct point (data/sparse-direct-point-value sparse row-key col-key))
    (assert-close! :point-map point (data/sparse-map-point-value sparse-map row-key col-key))
    (assert-close! :point-vectorz point (data/nested-vectorz-point-value nested-vectorz row-key col-key))
    (assert-close! :row-sum row-sum (data/sparse-row-map-sum sparse row-key))
    (assert-close! :row-sum-vectorz row-sum (data/nested-vectorz-row-map-sum nested-vectorz row-key))
    (assert-close! :col-sum col-sum (data/sparse-col-map-sum sparse scan-col))
    (assert-close! :row-only-col-sum col-sum row-only-col-sum)
    (println {:point point
              :row-sum row-sum
              :col-sum col-sum
              :row-only-col-sum row-only-col-sum})))

(defn smoke-check
  ([] (smoke-check (data/standard-configs)))
  ([configs]
   (println "sparse-layout benchmark smoke check")
   (doseq [config configs]
     (smoke-config config))))

(defn -main [& args]
  (let [configs (data/selected-configs args)]
    (if (some #{"smoke"} args)
      (smoke-check configs)
      (run-benchmarks configs (not (some #{"full"} args))))))
