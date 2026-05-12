(ns sparse-layout.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [sparse-layout.core :as sparse]
            [sparse-layout.core :refer [block col-blocks col-id defsparse row-blocks row-id]]))

(def ^Class double-array-type
  (Class/forName "[D"))

(defn- materialize-value [value]
  (if (instance? double-array-type value)
    (vec (seq value))
    value))

(defn- materialize-blocks [blocks]
  (mapv (fn [[key value]]
          [key (materialize-value value)])
        blocks))

(defsparse entity-features
  {:row-key [:entity]
   :cols-path [:vals]
   :payload {:kind :fixed-double-block
             :dim 3}
   :indices #{:csr :csc}})

(defsparse scalar-features
  {:row-key [:entity]
   :cols-path [:vals]
   :payload :double
   :duplicate-policy :sum
   :indices #{:csr :csc}})

(defsparse duplicate-error-features
  {:row-key [:entity]
   :cols-path [:vals]
   :payload :double
   :duplicate-policy :error
   :indices #{:csr}})

(defsparse object-merge-features
  {:row-key [:entity]
   :cols-path [:vals]
   :payload :object
   :duplicate-policy :merge
   :merge-fn merge
   :indices #{:csr}})

(defsparse variable-features
  {:row-key [:entity]
   :cols-path [:vals]
   :payload {:kind :var-double-block}
   :indices #{:csr :csc}})

(deftest compiles-fixed-double-block-layout
  (let [ds (entity-features-compile
            [{:entity 1
              :vals (array-map :f1 [1.0 2.0 3.0]
                               :f2 [4.0 5.0 6.0])}
             {:entity 2
              :vals (array-map :f2 [7.0 8.0 9.0])}])]
    (testing "dictionary ids are assigned during ingest"
      (is (= 0 (entity-features-row-id ds 1)))
      (is (= 1 (entity-features-row-id ds 2)))
      (is (= 0 (entity-features-col-id ds :f1)))
      (is (= 1 (entity-features-col-id ds :f2))))

    (testing "generated accessors use the compiled arrays"
      (is (= [1.0 2.0 3.0]
             (materialize-value (entity-features-block ds 1 :f1))))
      (is (= [4.0 5.0 6.0]
             (materialize-value (entity-features-block ds 1 :f2))))
      (is (nil? (entity-features-block ds 2 :f1))))

    (testing "CSR and CSC traversals return sparse blocks"
      (is (= [[:f1 [1.0 2.0 3.0]]
              [:f2 [4.0 5.0 6.0]]]
             (materialize-blocks (entity-features-row ds 1))))
      (is (= [[1 [4.0 5.0 6.0]]
              [2 [7.0 8.0 9.0]]]
             (materialize-blocks (entity-features-col ds :f2)))))

    (testing "the shared protocol works at API boundaries"
      (is (= 0 (row-id ds 1)))
      (is (= 1 (col-id ds :f2)))
      (is (= [7.0 8.0 9.0]
             (materialize-value (block ds 2 :f2))))
      (is (= [[:f1 [1.0 2.0 3.0]]
              [:f2 [4.0 5.0 6.0]]]
             (materialize-blocks (row-blocks ds 1))))
      (is (= [[1 [4.0 5.0 6.0]]
              [2 [7.0 8.0 9.0]]]
             (materialize-blocks (col-blocks ds :f2)))))))

(deftest duplicate-policy-sum-coalesces-scalar-values
  (let [ds (scalar-features-compile
            [{:entity :a :vals (array-map :x 1.25)}
             {:entity :a :vals (array-map :x 2.75)}
             {:entity :a :vals (array-map :y 10.0)}])]
    (is (= 4.0 (scalar-features-block ds :a :x)))
    (is (= [[:x 4.0] [:y 10.0]]
           (scalar-features-row ds :a)))))

(deftest duplicate-policy-error-rejects-duplicates
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Duplicate sparse coordinate"
       (duplicate-error-features-compile
        [{:entity :a :vals (array-map :x 1.0)}
         {:entity :a :vals (array-map :x 2.0)}]))))

(deftest duplicate-policy-merge-uses-merge-fn
  (let [ds (object-merge-features-compile
            [{:entity 1 :vals (array-map :meta {:a 1})}
             {:entity 1 :vals (array-map :meta {:b 2})}])]
    (is (= {:a 1 :b 2}
           (object-merge-features-block ds 1 :meta)))))

(deftest supports-variable-double-blocks
  (let [ds (variable-features-compile
            [{:entity 1 :vals (array-map :xs [1.0 2.0]
                                     :ys [3.0])}
             {:entity 2 :vals (array-map :xs (double-array [4.0 5.0 6.0]))}])]
    (is (= [1.0 2.0]
           (materialize-value (variable-features-block ds 1 :xs))))
    (is (= [[1 [1.0 2.0]]
            [2 [4.0 5.0 6.0]]]
           (materialize-blocks (variable-features-col ds :xs))))))

(deftest retain-coo-is-configurable-at-freeze-time
  (let [rows [{:entity 1 :vals (array-map :f1 [1.0 2.0 3.0]
                                          :f2 [4.0 5.0 6.0])}
              {:entity 2 :vals (array-map :f2 [7.0 8.0 9.0])}]
        no-coo-builder (sparse/make-builder
                        {:row-key [:entity]
                         :cols-path [:vals]
                         :payload {:kind :fixed-double-block :dim 3}
                         :indices #{:csr :csc}
                         :retain-coo? false})
        keep-coo-builder (sparse/make-builder
                          {:row-key [:entity]
                           :cols-path [:vals]
                           :payload {:kind :fixed-double-block :dim 3}
                           :indices #{:csr :csc}
                           :retain-coo? true})]
    (doseq [row rows]
      (let [entity (:entity row)]
        (doseq [[feature payload] (:vals row)]
          (sparse/append-entry! no-coo-builder entity feature payload)
          (sparse/append-entry! keep-coo-builder entity feature payload))))
    (let [no-coo (sparse/freeze-builder! no-coo-builder)
          keep-coo (sparse/freeze-builder! keep-coo-builder)]
      (is (nil? (:edge-rows no-coo)))
      (is (nil? (:edge-cols no-coo)))
      (is (some? (:csr-col-ids no-coo)))
      (is (some? (:csc-row-ids no-coo)))
      (is (some? (:csc-payload-ids no-coo)))
      (is (= [7.0 8.0 9.0]
             (materialize-value
              (sparse/block-by-ids 1 1
                                   (:csr-row-ptrs no-coo)
                                   (:csr-col-ids no-coo)
                                   (:csc-col-ptrs no-coo)
                                   (:csc-row-ids no-coo)
                                   (:csc-payload-ids no-coo)
                                   (:payload-kind no-coo)
                                   (:payload-dim no-coo)
                                   (:payload-values no-coo)
                                   (:payload-ptrs no-coo)))))
      (is (some? (:edge-rows keep-coo)))
      (is (some? (:edge-cols keep-coo))))))
