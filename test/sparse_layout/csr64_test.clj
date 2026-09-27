(ns sparse-layout.csr64-test
  (:require [clojure.test :refer [deftest is testing]]
            [sparse-layout.core :refer [defsparse]]
            [sparse-layout.csr-source :as csr]
            [sparse-layout.csr64 :as csr64]
            [sparse-layout.csr64-overlay :as overlay])
  (:import [java.nio.file Files Path]
           [java.util Arrays]))

(defsparse csr64-features
           {:row-key [:row]
            :cols-path [:vals]
            :payload {:kind :fixed-double-block :dim 3}
            :indices #{:csr}
            :retain-coo? false})

(defn- test-source
  []
  (-> [{:row 0 :vals (array-map 0 [1.0 2.0 3.0] 1 [4.0 5.0 6.0])}
       {:row 1 :vals (array-map 0 [7.0 8.0 9.0])} {:row 2 :vals (array-map 0 [10.0 11.0 12.0])}
       {:row 3 :vals (array-map 1 [13.0 14.0 15.0])}]
      csr64-features-compile
      csr/dataset->csr-source))

(defn- temp-artifact
  ^Path []
  (Files/createTempFile "sparse-layout-csr64-"
                        ".slcsr64"
                        (make-array java.nio.file.attribute.FileAttribute 0)))

(defn- with-artifact
  [source f]
  (let [path (temp-artifact)]
    (try (csr64/write-artifact! source path {:page-entries 2})
         (with-open [mapped (csr64/open-artifact path)]
           (f path mapped))
         (finally (Files/deleteIfExists path)))))

(defn- copied-range
  [source]
  (let [rows
        (int-array 5)

        cols
        (int-array 5)

        values
        (double-array 15)]

    (csr/csr-copy-ranges! source [[0 4]] rows cols values 0)
    {:rows (vec rows) :cols (vec cols) :values (vec values)}))

(deftest roundtrips-through-long-addressed-memory-segments
  (let [heap
        (test-source)

        expected
        (copied-range heap)]

    (with-artifact heap
                   (fn [path mapped]
                     (is (= {:row-count 4 :col-count 2 :entry-count 5 :block-dim 3}
                            (select-keys (csr64/metadata mapped)
                                         [:row-count :col-count :entry-count :block-dim])))
                     (is (= (Files/size path) (:file-size (csr64/metadata mapped))))
                     (is (= [0 2] (csr/csr-row-span mapped 0)))
                     (is (= expected (copied-range mapped)))
                     (is (= 5 (csr64/range-entry-count mapped 0 4)))
                     (is (= 0 (csr/csr-row-id mapped 0)))
                     (is (= -1 (csr/csr-row-id mapped :unknown)))))))

(deftest copies-bounded-pages-with-a-continuation-cursor
  (with-artifact (test-source)
                 (fn [_ mapped]
                   (let [rows
                         (int-array 4)

                         cols
                         (int-array 4)

                         values
                         (double-array 12)]

                     (Arrays/fill rows -1)
                     (Arrays/fill cols -1)
                     (Arrays/fill values -1.0)
                     (is (= 2 (csr64/copy-page! mapped 0 4 1 2 rows cols values 1)))
                     (is (= [-1 0 1 -1] (vec rows)))
                     (is (= [-1 1 0 -1] (vec cols)))
                     (is (= [-1.0 -1.0 -1.0 4.0 5.0 6.0 7.0 8.0 9.0 -1.0 -1.0 -1.0] (vec values)))
                     (is (= 2 (csr64/copy-page! mapped 0 4 3 8 rows cols values 0)))
                     (is (= [2 3 1 -1] (vec rows)))))))

(deftest page-copy-validates-before-writing
  (with-artifact (test-source)
                 (fn [_ mapped]
                   (let [rows (int-array 1)]
                     (Arrays/fill rows -1)
                     (is (thrown-with-msg?
                           clojure.lang.ExceptionInfo
                           #"destination"
                           (csr64/copy-page! mapped 0 2 0 3 rows (int-array 1) (double-array 3) 0)))
                     (is (= [-1] (vec rows)))))))

(deftest performs-binary-searched-point-copies
  (with-artifact (test-source)
                 (fn [_ mapped]
                   (let [values (double-array 5)]
                     (is (= 1 (csr64/find-entry mapped 0 1)))
                     (is (true? (csr64/copy-point! mapped 0 1 values 2)))
                     (is (= [0.0 0.0 4.0 5.0 6.0] (vec values)))
                     (is (= -1 (csr64/find-entry mapped 1 1)))
                     (is (nil? (csr64/copy-point! mapped 1 1 values 0)))))))

(deftest rejects-invalid-page-requests
  (with-artifact (test-source)
                 (fn [_ mapped]
                   (let [rows
                         (int-array 1)

                         cols
                         (int-array 1)

                         values
                         (double-array 3)]

                     (testing "cursor"
                       (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                             #"cursor exceeds"
                                             (csr64/copy-page! mapped 0 1 3 1 rows cols values 0))))
                     (testing "page size"
                       (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                             #"page size"
                                             (csr64/copy-page! mapped 0 1 0 0 rows cols values 0))))
                     (testing "range"
                       (is (thrown-with-msg?
                             clojure.lang.ExceptionInfo
                             #"row range"
                             (csr64/copy-page! mapped -1 1 0 1 rows cols values 0))))))))

(defn- ledger-fixture
  [replacement]
  [{:sequence 0 :op :put :row-id 0 :col-id 1 :block replacement}
   {:sequence 1 :op :delete :row-id 1 :col-id 0}
   {:sequence 2 :op :put :row-id 1 :col-id 1 :block (double-array [70.0 71.0 72.0])}
   {:sequence 3 :op :put :row-id 2 :col-id 0 :block (double-array [80.0 81.0 82.0])}
   {:sequence 4 :op :delete :row-id 2 :col-id 0} {:sequence 5 :op :delete :row-id 3 :col-id 0}
   {:sequence 6 :op :put :row-id 3 :col-id 0 :block (double-array [90.0 91.0 92.0])}])

(deftest compiles-an-immutable-last-write-wins-ledger-overlay
  (with-artifact
    (test-source)
    (fn [_ mapped]
      (let [replacement
            (double-array [40.0 41.0 42.0])

            view
            (overlay/compile-ledger mapped (ledger-fixture replacement))

            rows
            (int-array 5)

            cols
            (int-array 5)

            values
            (double-array 15)]

        (aset replacement 0 -1.0)
        (is (= {:through-sequence 6
                :ledger-record-count 7
                :retained-modification-count 5
                :structural-row-count 3
                :replacement-count 1
                :net-entry-adjustment 0}
               (overlay/metadata view)))
        (is (= 5 (overlay/range-entry-count view 0 4)))
        (is (= 3 (overlay/range-entry-count view 0 2)))
        (is (= 5 (overlay/copy-page! view 0 4 0 8 rows cols values 0)))
        (is (= [0 0 1 3 3] (vec rows)))
        (is (= [0 1 1 0 1] (vec cols)))
        (is (= [1.0 2.0 3.0 40.0 41.0 42.0 70.0 71.0 72.0 90.0 91.0 92.0 13.0 14.0 15.0]
               (vec values)))
        (doseq [cursor
                (range 6)

                page-size
                (range 1 4)]

          (let [copied
                (min page-size (- 5 cursor))

                page-rows
                (int-array copied)

                page-cols
                (int-array copied)

                page-values
                (double-array (* copied 3))]

            (is (=
                  copied
                  (overlay/copy-page! view 0 4 cursor page-size page-rows page-cols page-values 0)))
            (is (= (subvec (vec rows) cursor (+ cursor copied)) (vec page-rows)))
            (is (= (subvec (vec cols) cursor (+ cursor copied)) (vec page-cols)))
            (is (= (subvec (vec values) (* cursor 3) (* (+ cursor copied) 3))
                   (vec page-values)))))))))

(deftest pages-and-point-reads-across-ledger-structure-changes
  (with-artifact
    (test-source)
    (fn [_ mapped]
      (let [view
            (overlay/compile-ledger mapped (ledger-fixture (double-array [40.0 41.0 42.0])))

            rows
            (int-array 4)

            cols
            (int-array 4)

            values
            (double-array 12)

            point
            (double-array 3)]

        (Arrays/fill rows -1)
        (Arrays/fill cols -1)
        (Arrays/fill values -1.0)
        (is (= 2 (overlay/copy-page! view 0 4 1 2 rows cols values 1)))
        (is (= [-1 0 1 -1] (vec rows)))
        (is (= [-1 1 1 -1] (vec cols)))
        (is (= [40.0 41.0 42.0 70.0 71.0 72.0] (subvec (vec values) 3 9)))
        (is (= 2 (overlay/copy-page! view 0 4 3 8 rows cols values 0)))
        (is (= [3 3] (subvec (vec rows) 0 2)))
        (is (= [0 1] (subvec (vec cols) 0 2)))
        (is (true? (overlay/copy-point! view 0 1 point 0)))
        (is (= [40.0 41.0 42.0] (vec point)))
        (is (nil? (overlay/copy-point! view 1 0 point 0)))
        (is (true? (overlay/copy-point! view 1 1 point 0)))
        (is (= [70.0 71.0 72.0] (vec point)))
        (is (true? (overlay/copy-point! view 3 0 point 0)))
        (is (= [90.0 91.0 92.0] (vec point)))
        (is (true? (overlay/copy-point! view 3 1 point 0)))
        (is (= [13.0 14.0 15.0] (vec point)))))))

(deftest ledger-overlay-resumes-bulk-copy-after-a-structural-row
  (with-artifact (test-source)
                 (fn [_ mapped]
                   (let [view
                         (overlay/compile-ledger mapped
                                                 [{:sequence 0 :op :delete :row-id 1 :col-id 0}])

                         rows
                         (int-array 2)

                         cols
                         (int-array 2)

                         values
                         (double-array 6)]

                     (is (= 2 (overlay/copy-page! view 0 4 2 2 rows cols values 0)))
                     (is (= [2 3] (vec rows)))
                     (is (= [0 1] (vec cols)))
                     (is (= [10.0 11.0 12.0 13.0 14.0 15.0] (vec values)))))))

(deftest ledger-overlay-validates-before-writing
  (let [heap (test-source)]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"require a CSR64Source"
                          (overlay/compile-ledger heap []))))
  (with-artifact (test-source)
                 (fn [_ mapped]
                   (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                         #"strictly increasing"
                                         (overlay/compile-ledger
                                           mapped
                                           [{:sequence 0 :op :delete :row-id 0 :col-id 0}
                                            {:sequence 0 :op :delete :row-id 0 :col-id 1}])))
                   (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                         #"out of bounds"
                                         (overlay/compile-ledger
                                           mapped
                                           [{:sequence 0 :op :delete :row-id 4 :col-id 0}])))
                   (is (thrown-with-msg?
                         clojure.lang.ExceptionInfo
                         #"wrong dimension"
                         (overlay/compile-ledger
                           mapped
                           [{:sequence 0 :op :put :row-id 0 :col-id 0 :block (double-array 2)}])))
                   (let [view
                         (overlay/compile-ledger mapped [])

                         rows
                         (int-array 1)]

                     (Arrays/fill rows -1)
                     (is (thrown-with-msg?
                           clojure.lang.ExceptionInfo
                           #"destination"
                           (overlay/copy-page! view 0 2 0 3 rows (int-array 1) (double-array 3) 0)))
                     (is (= [-1] (vec rows)))))))
