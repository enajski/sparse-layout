(ns sparse-layout.notebook-data
  (:require [sparse-layout.core :refer [defsparse]]))

(defsparse portfolio-features
           {:row-key [:row]
            :cols-path [:features]
            :payload {:kind :fixed-double-block :dim 3}
            :indices #{:csr :csc}})

(def records
  [{:row [:acme :core :fx]
    :features
    (array-map :exposure [12.0 0.91 1.0] :risk [0.42 0.78 1.0] :liquidity [0.88 0.83 1.0])}
   {:row [:acme :core :rates] :features (array-map :exposure [8.0 0.86 2.0] :risk [0.31 0.74 2.0])}
   {:row [:acme :growth :equity]
    :features (array-map :exposure [19.0 0.88 1.0] :momentum [1.12 0.67 3.0])}
   {:row [:globex :core :fx]
    :features (array-map :exposure [15.0 0.89 1.0] :liquidity [0.73 0.70 2.0])}
   {:row [:globex :income :credit]
    :features (array-map :risk [0.57 0.81 1.0] :carry [0.19 0.76 2.0])}])

(def dataset (portfolio-features-compile records))
