(ns sparse-layout.flatiron-compare-test
  (:require [clojure.test :refer [deftest is]]
            [sparse-layout.bench-data :as data]
            [sparse-layout.csr-source :as csr]
            [sparse-layout.flatiron-compare :as flatiron])
  (:import [java.util Arrays]))

(deftest matched-block-copies
  (doseq [shape [:scalar :compound-map]]
    (let [entries (data/make-entries {:key-shape shape :row-count 7 :col-count 11 :nnz-per-row 3})
          source (csr/dataset->csr-source (data/entries->sparse (reverse entries)))
          adapted (flatiron/from-source source)]

      (doseq [[start end] [[0 0] [0 7] [2 3] [3 7] [7 7]]]
        (let [n (* (- end start) 3)
              expected
              {:rows (int-array n) :cols (int-array n) :values (double-array (* n data/block-dim))}
              actual
              {:rows (int-array n) :cols (int-array n) :values (double-array (* n data/block-dim))}
              run (flatiron/runner adapted start end actual)]

          (is (= n
                 (csr/csr-copy-ranges! source
                                       [[start end]]
                                       (:rows expected)
                                       (:cols expected)
                                       (:values expected)
                                       0)))
          (dotimes [_ 2]
            (Arrays/fill ^doubles (:values actual) Double/NaN)
            (is (= n (run)))
            (is (Arrays/equals ^ints (:rows expected) ^ints (:rows actual)))
            (is (Arrays/equals ^ints (:cols expected) ^ints (:cols actual)))
            (is (Arrays/equals ^doubles (:values expected) ^doubles (:values actual)))))))))
