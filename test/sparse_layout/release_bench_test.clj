(ns sparse-layout.release-bench-test
  (:require [clojure.test :refer [deftest is]]
            [sparse-layout.release-bench :as bench]))

(deftest fixture-and-operations
  (let [fixture
        (bench/fixture true)

        cases
        (bench/cases fixture)]

    (is (= 16 (count (:entries fixture))))
    (is (= [:ingest-freeze :freeze :point-copy :row-copy :row-view :col-view] (mapv :id cases)))
    (is (bench/verify-dataset! fixture (:dataset fixture)))
    (doseq [{:keys [f check]} cases]
      (is (true? (check (f)))))))

(deftest exact-oracle-rejects-wrong-results
  (is (thrown? clojure.lang.ExceptionInfo (bench/check-equal! :guard [1.0] [2.0])))
  (let [point (nth (bench/cases (bench/fixture true)) 2)]
    (is (thrown? clojure.lang.ExceptionInfo ((:check point) (double-array 8))))))

(deftest smoke-and-check-structure
  (let [report (bench/run-benchmark {:smoke? true :warmup-ms 0 :samples 2})]
    (is (= :passed (:oracle report)))
    (is (= 6 (count (:cases report))))
    (is (every? #(= 2 (count (:samples %))) (:cases report)))
    (is (every? #(= 1 (:batch %)) (:cases report)))
    (is (string? (get-in report [:environment :core-sha256]))))
  (let [report (bench/run-benchmark {:smoke? true :check-only? true})]
    (is (= :passed (:oracle report)))
    (is (nil? (:cases report)))))

(deftest invalid-options
  (doseq [options [{:samples 0} {:samples 3} {:warmup-ms 0} {:warmup-ms -1}]]
    (is (thrown? clojure.lang.ExceptionInfo (bench/run-benchmark options)))))
