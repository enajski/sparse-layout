(ns sparse-layout.csr64-probe
  "Uses `reduce-entries` without importing java.lang.foreign; `csr64-test` loads it with
  reflection and boxed-math warnings enabled and expects none."
  (:require [sparse-layout.csr64 :as csr64]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn lane-0-sum
  "Sums lane 0 over every entry."
  ^double [source]
  (csr64/reduce-entries [_row _col e] [source] [acc 0.0] (+ acc (csr64/lane source e 0))))

(defn row-col-sum
  "Sums row id plus column id over a row range, in a primitive long accumulator."
  ^long [source row-start row-end]
  (csr64/reduce-entries [row col _e] [source row-start row-end] [acc 0] (+ acc row col)))
