(ns sparse-layout.csr64-overlay-bench-test
  (:require [clojure.test :refer [deftest is]]
            [sparse-layout.csr64-overlay-bench :as bench]))

(deftest independent-ledger-replay
  (let [records
        (bench/ledger 1 4 2 :mixed)

        result
        (bench/oracle 1 4 2 records)]

    (is (= [[0 1] [0 2] [0 5] [0 6] [0 8]] (mapv first result)))
    (is (= [-41.0 -40.9375] (second (nth result 1))))))

(deftest all-tiny-cases-agree
  (bench/with-cases true
                    (fn [cases compile-results]
                      (is (= 9 (count cases)))
                      (is (= 5 (count compile-results)))
                      (doseq [case cases]
                        (is (true? (bench/verify-case! case)))))))

(deftest oracle-detects-corruption-and-sentinels
  (let [expected
        [[[0 2] [1.0 2.0]]]

        dst
        (bench/destination 2 2)

        rows
        ^ints (:rows dst)

        cols
        ^ints (:cols dst)

        values
        ^doubles (:values dst)]

    (aset rows 2 0)
    (aset cols 2 2)
    (aset values 4 1.0)
    (aset values 5 2.0)
    (is (bench/check-page! expected 0 1 dst))
    (aset values 5 3.0)
    (is (thrown? clojure.lang.ExceptionInfo (bench/check-page! expected 0 1 dst)))
    (aset values 5 2.0)
    (aset rows 0 0)
    (is (thrown? clojure.lang.ExceptionInfo (bench/check-page! expected 0 1 dst)))
    (is (thrown? clojure.lang.ExceptionInfo (bench/check-page! expected 0 0 dst)))))

(deftest benchmark-options-reject-misleading-runs
  (doseq [options [{:samples 3} {:warmup-ms 0} {:batch 0}]]
    (is (thrown? clojure.lang.ExceptionInfo (bench/run-benchmark options)))))

(deftest dense-cost-ledger-and-oracle
  (let [records (bench/dense-ledger 4 2)]
    (is (= [0 4 8] (mapv :col-id records)))
    (is (= [[0 0] [0 2] [0 4] [0 6] [0 8]] (mapv first (bench/oracle 1 4 2 records)))))
  (bench/with-cost-cases true
                         (fn [fixtures]
                           (is (= [:dense :tail] (mapv :id fixtures)))
                           (doseq [{:keys [case modifications]} fixtures]
                             (is (bench/verify-case! case))
                             ;; Mutating caller-owned blocks must not alter the compiled snapshot.
                             (doseq [record modifications]
                               (java.util.Arrays/fill ^doubles (:block record) 123456.0))
                             (is (bench/verify-case! case))))))

(deftest cost-mode-options-reject-misleading-runs
  (doseq [options [{:samples 3} {:warmup-ms 0} {:batch 0}]]
    (is (thrown? clojure.lang.ExceptionInfo (bench/run-cost-benchmark options)))))
