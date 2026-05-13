(ns sparse-layout.facade-test
  (:require [clojure.test :refer [deftest is testing]]
            [sparse-layout.core :as sparse]
            [sparse-layout.core-test :as fixtures]
            [sparse-layout.facade :as facade]))

(def ^Class double-array-type
  (Class/forName "[D"))

(defn- materialize-value [value]
  (if (instance? double-array-type value)
    (vec (seq value))
    value))

(defn- materialize-view [view]
  (sparse/block-view->vec view))

(deftest dataset-and-row-facades-behave-like-read-maps
  (let [ds (fixtures/entity-features-compile
            [{:entity 1
              :vals (array-map :f1 [1.0 2.0 3.0]
                               :f2 [4.0 5.0 6.0])}
             {:entity 2
              :vals (array-map :f2 [7.0 8.0 9.0])}])
        m (facade/as-map ds)
        row (get m 1)]
    (testing "dataset lookup"
      (is (= 2 (count m)))
      (is (contains? m 1))
      (is (not (contains? m 99)))
      (is (nil? (get m 99)))
      (is (= 1 (key (find m 1))))
      (is (= #{1 2} (set (keys m)))))

    (testing "nested row lookup"
      (is (= 2 (count row)))
      (is (contains? row :f1))
      (is (not (contains? row :missing)))
      (is (= [4.0 5.0 6.0]
             (materialize-value (get row :f2))))
      (is (= [4.0 5.0 6.0]
             (materialize-value (get-in m [1 :f2]))))
      (is (= [:f1 :f2] (mapv key (seq row)))))

    (testing "reduce-kv walks sparse entries only"
      (is (= {1 #{:f1 :f2}
              2 #{:f2}}
             (reduce-kv (fn [acc entity row]
                          (assoc acc entity (set (keys row))))
                        {}
                        m)))
      (is (= {:f1 [1.0 2.0 3.0]
              :f2 [4.0 5.0 6.0]}
             (reduce-kv (fn [acc feature payload]
                          (assoc acc feature (materialize-value payload)))
                        {}
                        row))))))

(deftest column-facade-is-row-keyed
  (let [ds (fixtures/entity-features-compile
            [{:entity 1
              :vals (array-map :f1 [1.0 2.0 3.0]
                               :f2 [4.0 5.0 6.0])}
             {:entity 2
              :vals (array-map :f2 [7.0 8.0 9.0])}])
        col (facade/col-map ds :f2)]
    (is (= 2 (count col)))
    (is (contains? col 1))
    (is (not (contains? col 3)))
    (is (= [7.0 8.0 9.0]
           (materialize-value (get col 2))))
    (is (= [[1 [4.0 5.0 6.0]]
            [2 [7.0 8.0 9.0]]]
           (mapv (fn [[entity payload]]
                   [entity (materialize-value payload)])
                 col)))
    (is (= {1 [4.0 5.0 6.0]
            2 [7.0 8.0 9.0]}
           (reduce-kv (fn [acc entity payload]
                        (assoc acc entity (materialize-value payload)))
                      {}
                      col)))))

(deftest view-facades-return-zero-copy-block-views
  (let [ds (fixtures/entity-features-compile
            [{:entity 1
              :vals (array-map :f1 [1.0 2.0 3.0]
                               :f2 [4.0 5.0 6.0])}
             {:entity 2
              :vals (array-map :f2 [7.0 8.0 9.0])}])
        m (facade/as-view-map ds)
        row (facade/row-view-map ds 1)
        col (facade/col-view-map ds :f2)]
    (is (= [4.0 5.0 6.0]
           (materialize-view (get-in m [1 :f2]))))
    (is (= [1.0 2.0 3.0]
           (materialize-view (get row :f1))))
    (is (= [7.0 8.0 9.0]
           (materialize-view (get col 2))))))

(deftest scalar-view-facades-reject-block-views
  (let [ds (fixtures/scalar-features-compile
            [{:entity :a :vals (array-map :x 1.25)}])
        m (facade/as-view-map ds)]
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Zero-copy block views require block payload storage"
         (get-in m [:a :x])))))
