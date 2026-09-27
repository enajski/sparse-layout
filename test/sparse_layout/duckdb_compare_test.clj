(ns sparse-layout.duckdb-compare-test
  (:require [clojure.test :refer [deftest is testing]]
            [sparse-layout.duckdb-compare :as compare]
            [sparse-layout.flatiron-compare :as flatiron])
  (:import [java.util Arrays]))

(defn- comparison-selection
  [pages-builder]
  (let [good-builder
        (fn [_ ^long _ ^long _ destination]
          (let [edge-count (alength ^ints (:rows destination))]
            (fn []
              (Arrays/fill ^ints (:rows destination) 0)
              (Arrays/fill ^ints (:cols destination) 1)
              (Arrays/fill ^doubles (:values destination) 2.0)
              edge-count)))

        parquet-builder
        (fn [& args]
          (good-builder nil 0 0 (last args)))

        bad-pages-builder
        (fn [_ ^long _ ^long _ destination]
          (pages-builder destination))]

    (with-redefs-fn {#'compare/sparse-bulk-runner good-builder
                     #'compare/csr64-runner good-builder
                     #'compare/csr64-pages-runner bad-pages-builder
                     #'compare/csr64-reduce-entries-runner good-builder
                     #'compare/sparse-visitor-runner good-builder
                     #'flatiron/runner good-builder
                     #'compare/projected-runner parquet-builder
                     #'compare/long-runner parquet-builder
                     #'compare/measure (fn [& _]
                                         {})}
      #(#'compare/run-selection
         nil
         nil
         nil
         nil
         {}
         {:nnz-per-row 1}
         {:selection :full :start 0 :end 1}
         true))))

(deftest untimed-validation-does-not-trust-reused-destinations
  (let [bad-builders {:no-op (fn [destination]
                               (let [edge-count (alength ^ints (:rows destination))]
                                 (fn []
                                   edge-count)))
                      :partial (fn [destination]
                                 (let [edge-count (alength ^ints (:rows destination))]
                                   (fn []
                                     (Arrays/fill ^ints (:rows destination) 0)
                                     edge-count)))}]
    (doseq [[kind bad-builder] bad-builders]
      (testing (name kind)
        (is (thrown-with-msg? AssertionError
                              #"csr64-pages output differs from sparse CSR"
                              (comparison-selection bad-builder)))))))

(deftest untimed-validation-accepts-complete-writers
  (is (= 9
         (count (comparison-selection (fn [destination]
                                        (let [edge-count (alength ^ints (:rows destination))]
                                          (fn []
                                            (Arrays/fill ^ints (:rows destination) 0)
                                            (Arrays/fill ^ints (:cols destination) 1)
                                            (Arrays/fill ^doubles (:values destination) 2.0)
                                            edge-count))))))))
