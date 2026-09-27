(ns sparse-layout.csr64-test
  (:require [clojure.test :refer [deftest is testing]]
            [sparse-layout.core :refer [defsparse]]
            [sparse-layout.csr-source :as csr]
            [sparse-layout.csr64 :as csr64]
            [sparse-layout.csr64-overlay :as overlay])
  (:import [java.nio ByteOrder]
           [java.nio.file Files Path]
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

(defn- gapped-source
  "Four rows with block dim 2; rows 1 and 3 hold no entries."
  []
  (csr/->HeapCSRSource {0 0 1 1 2 2 3 3}
                       (object-array [0 1 2 3])
                       {0 0 1 1 2 2}
                       (object-array [0 1 2])
                       (int-array [0 1 1 3 3])
                       (int-array [1 0 2])
                       (double-array [1.0 2.0 3.0 4.0 5.0 6.0])
                       2
                       nil
                       nil))

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

(defn- copied-rows
  [source row-start row-end]
  (let [entry-count
        (csr64/range-entry-count source row-start row-end)

        rows
        (int-array entry-count)

        cols
        (int-array entry-count)

        values
        (double-array (* entry-count (csr64/block-dim source)))]

    (csr/csr-copy-ranges! source [[row-start row-end]] rows cols values 0)
    {:rows (vec rows) :cols (vec cols) :values (vec values)}))

(defn- drained
  "Rebuilds every entry a reduce over `reducible` hands out, through the page accessors."
  [block-dim reducible]
  (reduce (fn [acc page]
            (reduce (fn [{:keys [rows cols values]} i]
                      {:rows (conj rows (csr64/page-row page i))
                       :cols (conj cols (csr64/page-col page i))
                       :values (into values (map #(csr64/page-lane page i %) (range block-dim)))})
                    acc
                    (range (count page))))
          {:rows [] :cols [] :values []}
          reducible))

(defn- scanned
  "Every (row col e) triple `csr-scan-ranges!` visits over `ranges`."
  [source ranges]
  (let [triples (atom [])]
    (csr/csr-scan-ranges! source
                          ranges
                          (fn [row col e _]
                            (swap! triples conj [row col e])))
    @triples))

(defn- block-sum
  "Sums every lane of entry `e` through `csr64/lane`."
  ^double [source ^long e]
  (let [dim (csr64/block-dim source)]
    (loop [k 0
           sum 0.0]

      (if (< k dim) (recur (inc k) (+ sum (csr64/lane source e k))) sum))))

(defn- copy-entries!
  "The README's whole-block loop: one `csr-copy-block!` per entry; returns the entry count."
  [mapped ^ints row-ids ^ints col-ids ^doubles values row-start row-end]
  (let [dim (csr64/block-dim mapped)]
    (csr64/reduce-entries [row col e]
                          [mapped row-start row-end]
                          [slot 0]
                          (aset row-ids slot (int row))
                          (aset col-ids slot (int col))
                          (csr/csr-copy-block! mapped e values (* slot dim))
                          (inc slot))))

(defn- thrown-info
  "Returns the message and data of the `ExceptionInfo` that `f` throws, or nil."
  [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e [(ex-message e) (ex-data e)])))

(defn- point-copy
  "Calls `copy!` with a fresh block of `dim` doubles; returns whether it returned that block (nil
  when it returned nil) and the block's values."
  [dim copy!]
  (let [dst
        (double-array dim)

        result
        (copy! dst)]

    [(when result (identical? dst result)) (vec dst)]))

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

(deftest empty-pages-return-zero-without-writing
  (with-artifact (test-source)
                 (fn [_ mapped]
                   (let [rows
                         (int-array 2 -1)

                         cols
                         (int-array 2 -1)

                         values
                         (double-array 6 -1.0)]

                     (is (= 0 (csr64/copy-page! mapped 2 2 0 2 rows cols values 0)))
                     (is (= 0 (csr64/copy-page! mapped 0 4 5 2 rows cols values 0)))
                     (is (= [-1 -1] (vec rows) (vec cols)))
                     (is (= (repeat 6 -1.0) (vec values)))))))

(deftest performs-binary-searched-point-copies
  (with-artifact (test-source)
                 (fn [_ mapped]
                   (let [values (double-array 5)]
                     (is (= 1 (csr64/find-entry mapped 0 1)))
                     (is (identical? values (csr64/copy-point! mapped 0 1 values 2)))
                     (is (= [0.0 0.0 4.0 5.0 6.0] (vec values)))
                     (is (= -1 (csr64/find-entry mapped 1 1)))
                     (is (nil? (csr64/copy-point! mapped 1 1 values 0)))))))

(deftest point-reads-reject-out-of-range-ids
  (with-artifact (test-source)
                 (fn [_ mapped]
                   (let [values (double-array 3)]
                     (doseq [[row-id col-id] [[-1 0] [4 0] [0 -1] [0 2]]]
                       (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                             #"out of bounds"
                                             (csr64/find-entry mapped row-id col-id)))
                       (is (thrown-with-msg?
                             clojure.lang.ExceptionInfo
                             #"out of bounds"
                             (csr64/copy-point! mapped row-id col-id values 0))))))))

(deftest point-reads-name-the-first-out-of-range-id
  (with-artifact (test-source)
                 (fn [_ mapped]
                   (doseq [[row-id col-id data] [[-1 0 {:index :row-id :value -1 :upper-bound 4}]
                                                 [4 0 {:index :row-id :value 4 :upper-bound 4}]
                                                 [0 -1 {:index :col-id :value -1 :upper-bound 2}]
                                                 [0 2 {:index :col-id :value 2 :upper-bound 2}]
                                                 [-1 2 {:index :row-id :value -1 :upper-bound 4}]]]
                     (is (= ["CSR64 index is out of bounds." data]
                            (thrown-info #(csr64/find-entry mapped row-id col-id))))))))

(deftest four-argument-copy-point-copies-to-offset-zero
  (is (instance? clojure.lang.IFn$OLLOO csr64/copy-point!))
  (doseq [[source [present-row present-col]] [[(test-source) [0 1]] [(gapped-source) [2 2]]]]
    (with-artifact
      source
      (fn [_ mapped]
        (let [dim (csr64/block-dim mapped)
              row-count (csr64/row-count mapped)
              col-count (csr64/col-count mapped)]

          (doseq [row-id (range row-count)
                  col-id (range col-count)]

            (is (= (point-copy dim #(csr64/copy-point! mapped row-id col-id % 0))
                   (point-copy dim #(csr64/copy-point! mapped row-id col-id %)))))
          (doseq [[row-id col-id dst] [[-1 0 (double-array dim)] [row-count 0 (double-array dim)]
                                       [0 -1 (double-array dim)] [0 col-count (double-array dim)]
                                       [present-row present-col (double-array (dec dim))]]]
            (let [thrown (thrown-info #(csr64/copy-point! mapped row-id col-id dst 0))]
              (is (some? thrown))
              (is (= thrown (thrown-info #(csr64/copy-point! mapped row-id col-id dst)))))))))))

(deftest reading-requires-a-little-endian-host
  (is (nil? (#'csr64/ensure-little-endian-host! ByteOrder/LITTLE_ENDIAN)))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"little-endian"
                        (#'csr64/ensure-little-endian-host! ByteOrder/BIG_ENDIAN))))

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

(deftest drains-row-ranges-through-reusable-pages
  (with-artifact
    (test-source)
    (fn [_ mapped]
      (is (= [4 2 5 3]
             ((juxt csr64/row-count csr64/col-count csr64/entry-count csr64/block-dim) mapped)))
      (doseq [[row-start row-end]
              [[0 4] [1 3] [2 2]]

              capacity
              [1 2 5 8]]

        (testing (pr-str [row-start row-end capacity])
          (is (= (copied-rows mapped row-start row-end)
                 (drained 3 (csr64/pages mapped row-start row-end (csr64/page mapped capacity)))))))
      (is (= (copied-rows mapped 0 4) (drained 3 (csr64/pages mapped (csr64/page mapped 2)))))
      (is (= 5 (transduce (map count) + 0 (csr64/pages mapped (csr64/page mapped 2)))))))
  (with-artifact (gapped-source)
                 (fn [_ mapped]
                   (doseq [capacity [1 2 3]]
                     (is (= (copied-rows mapped 0 4)
                            (drained 2 (csr64/pages mapped (csr64/page mapped capacity)))))))))

(deftest reduced-stops-the-drain-before-the-next-copy
  (with-artifact (test-source)
                 (fn [_ mapped]
                   (let [buf
                         (csr64/page mapped 2)

                         calls
                         (atom 0)]

                     (is (= :stopped
                            (reduce (fn [_ _]
                                      (swap! calls inc)
                                      (reduced :stopped))
                                    nil
                                    (csr64/pages mapped buf))))
                     (is (= 1 @calls))
                     (is (= 2 (count buf)))
                     (is (= [0 0] (vec (csr64/page-rows buf))))
                     (is (= [0 1] (vec (csr64/page-cols buf))))
                     (is (= [1.0 2.0 3.0 4.0 5.0 6.0] (vec (csr64/page-values buf))))))))

(deftest pages-validate-their-arguments
  (with-artifact
    (test-source)
    (fn [_ mapped]
      (let [buf (csr64/page mapped 2)]
        (testing "row range"
          (doseq [[row-start row-end] [[-1 2] [3 2] [0 5]]]
            (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                  #"row range"
                                  (csr64/pages mapped row-start row-end buf)))))
        (testing "block dim"
          (with-artifact (gapped-source)
                         (fn [_ gapped]
                           (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                                 #"block dim"
                                                 (csr64/pages mapped (csr64/page gapped 2))))
                           (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                                 #"block dim"
                                                 (csr64/pages gapped buf))))))
        (testing "capacity"
          (doseq [capacity [0 -1 (inc (quot Integer/MAX_VALUE 3)) Long/MAX_VALUE]]
            (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                  #"capacity"
                                  (csr64/page mapped capacity)))))
        (testing "seq" (is (thrown? IllegalArgumentException (seq (csr64/pages mapped buf)))))))))

(deftest page-accessors-reject-out-of-range-indexes
  (with-artifact
    (test-source)
    (fn [_ mapped]
      (let [buf (csr64/page mapped 2)]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"out of bounds" (csr64/page-row buf 0)))
        (reduce (fn [_ _]
                  (reduced nil))
                nil
                (csr64/pages mapped buf))
        (doseq [i [-1 2]]
          (is (thrown-with-msg? clojure.lang.ExceptionInfo #"out of bounds" (csr64/page-row buf i)))
          (is (thrown-with-msg? clojure.lang.ExceptionInfo #"out of bounds" (csr64/page-col buf i)))
          (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                #"out of bounds"
                                (csr64/page-lane buf i 0))))
        (doseq [k [-1 3]]
          (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                #"out of bounds"
                                (csr64/page-lane buf 1 k))))
        (is (= 6.0 (csr64/page-lane buf 1 2)))))))

(deftest prints-only-the-shape
  (with-artifact
    (test-source)
    (fn [path mapped]
      (let [shape
            (str "{:path "
                 (pr-str (str path))
                 ", :row-count 4, :col-count 2, :entry-count 5, :block-dim 3, :open? ")

            closed
            (with-open [source (csr64/open-artifact path)]
              source)]

        (is (= (str "#object[sparse_layout.csr64.CSR64Source " shape "true}]") (pr-str mapped)))
        (is (= (str "#object[sparse_layout.csr64.CSR64Source " shape "false}]") (pr-str closed)))
        (is (= "#object[sparse_layout.csr64.Page {:count 0, :capacity 8, :block-dim 3}]"
               (pr-str (csr64/page mapped 8))))))))

(deftest reduces-entries-in-scan-order
  (doseq [[source whole] [[(test-source) [[0 0 0] [0 1 1] [1 0 2] [2 0 3] [3 1 4]]]
                          [(gapped-source) [[0 1 0] [2 0 1] [2 2 2]]]]]
    (with-artifact
      source
      (fn [_ mapped]
        (doseq [[row-start row-end] [[0 4] [1 3] [2 2]]]
          (testing (pr-str [row-start row-end])
            (is (= (scanned mapped [[row-start row-end]])
                   (csr64/reduce-entries [row col e]
                                         [mapped row-start row-end]
                                         [acc []]
                                         (conj acc [row col e]))))))
        (is (= whole
               (scanned mapped [[0 4]])
               (csr64/reduce-entries [row col e] [mapped] [acc []] (conj acc [row col e]))))))))

(deftest lanes-read-the-copied-values
  (doseq [source [(test-source) (gapped-source)]]
    (with-artifact source
                   (fn [_ mapped]
                     (let [values (:values (copied-rows mapped 0 4))]
                       (is (= values
                              (vec (for [e (range (csr64/entry-count mapped))
                                         k (range (csr64/block-dim mapped))]

                                     (csr64/lane mapped e k)))))
                       (is (= (reduce + values)
                              (csr64/reduce-entries [_row _col e]
                                                    [mapped]
                                                    [acc 0.0]
                                                    (+ acc (block-sum mapped e))))))))))

(deftest reduce-entries-copies-whole-blocks-like-copy-page
  (doseq [source [(test-source) (gapped-source)]]
    (with-artifact
      source
      (fn [_ mapped]
        (doseq [[row-start row-end] [[0 4] [0 2] [1 3] [2 4] [3 4] [2 2]]]
          (testing (pr-str [row-start row-end])
            (let [capacity (max 1 (csr64/range-entry-count mapped row-start row-end))
                  copied (fn [copy!]
                           (let [rows (int-array capacity -1)
                                 cols (int-array capacity -1)
                                 values (double-array (* capacity (csr64/block-dim mapped)) -1.0)]

                             {:count (copy! rows cols values)
                              :rows (vec rows)
                              :cols (vec cols)
                              :values (vec values)}))]

              (is (= (copied #(csr64/copy-page! mapped row-start row-end 0 capacity %1 %2 %3 0))
                     (copied #(copy-entries! mapped %1 %2 %3 row-start row-end)))))))))))

(deftest reduce-entries-evaluates-its-arguments-once
  (with-artifact
    (test-source)
    (fn [_ mapped]
      (let [calls
            (atom [])

            track
            (fn [label value]
              (swap! calls conj label)
              value)]

        (is (= 2
               (csr64/reduce-entries [_row _col _e]
                                     [(track :src mapped) (track :row-start 1) (track :row-end 3)]
                                     [acc (track :init 0)]
                                     (inc acc))))
        (is (= [:src :row-start :row-end :init] @calls))
        (testing "invalid ranges throw before init"
          (doseq [[row-start row-end] [[-1 2] [3 2] [0 5]]]
            (reset! calls [])
            (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                  #"row range"
                                  (csr64/reduce-entries [_row _col _e]
                                                        [mapped row-start row-end]
                                                        [acc (track :init 0)]
                                                        (inc acc))))
            (is (= [] @calls))))
        (testing "malformed bindings"
          (doseq [form ['(sparse-layout.csr64/reduce-entries [row col] [src] [acc 0] acc)
                        '(sparse-layout.csr64/reduce-entries [row col e] [src 0] [acc 0] acc)
                        '(sparse-layout.csr64/reduce-entries [row col e] [src] [acc] acc)]]
            (let [cause (try (macroexpand-1 form)
                             (catch clojure.lang.Compiler$CompilerException e (ex-cause e)))]
              (is (instance? clojure.lang.ExceptionInfo cause))
              (is (re-find #"reduce-entries expects" (str (ex-message cause)))))))))))

(deftest lane-rejects-out-of-range-indexes
  (with-artifact
    (test-source)
    (fn [path mapped]
      (doseq [e [-1 5]]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"out of bounds" (csr64/lane mapped e 0))))
      (doseq [k [-1 3]]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"out of bounds" (csr64/lane mapped 4 k))))
      (is (= 15.0 (csr64/lane mapped 4 2)))
      (testing "closed source"
        (let [closed (with-open [source (csr64/open-artifact path)]
                       source)]
          (is (thrown? IllegalStateException (csr64/lane closed 0 0)))
          (is (thrown? IllegalStateException
                       (csr64/reduce-entries [_row _col _e] [closed] [acc 0] (inc acc)))))))))

(deftest direct-lane-calls-match-calls-through-the-var
  (doseq [source [(test-source) (gapped-source)]]
    (with-artifact
      source
      (fn [path mapped]
        (let [entries (csr64/entry-count mapped)
              dim (csr64/block-dim mapped)]

          (doseq [e (range entries)
                  k (range dim)]

            (is (= (csr64/lane mapped e k)
                   (apply csr64/lane [mapped e k])
                   (#'csr64/lane mapped e k))))
          (doseq [[e k data] [[-1 0 {:index :entry-id :value -1 :upper-bound entries}]
                              [entries 0 {:index :entry-id :value entries :upper-bound entries}]
                              [0 -1 {:index :lane :value -1 :upper-bound dim}]
                              [0 dim {:index :lane :value dim :upper-bound dim}]]]
            (is (= ["CSR64 index is out of bounds." data]
                   (thrown-info #(csr64/lane mapped e k))
                   (thrown-info #(apply csr64/lane [mapped e k]))
                   (thrown-info #(#'csr64/lane mapped e k)))))
          (testing "a redefinition reaches calls through the var, not compiled direct calls"
            (let [value (csr64/lane mapped 0 0)]
              (with-redefs [csr64/lane (constantly -1.0)]
                (is (= value (csr64/lane mapped 0 0)))
                (is (= -1.0 (apply csr64/lane [mapped 0 0]) (#'csr64/lane mapped 0 0))))))
          (testing "closed source"
            (let [closed (with-open [reopened (csr64/open-artifact path)]
                           reopened)]
              (is (thrown? IllegalStateException (csr64/lane closed 0 0)))
              (is (thrown? IllegalStateException (apply csr64/lane [closed 0 0])))
              (is (thrown? IllegalStateException (#'csr64/lane closed 0 0))))))))))

(deftest reduce-entries-compiles-without-reflection-or-boxing
  (let [warnings (java.io.StringWriter.)]
    (binding [*err* warnings]
      (load "/sparse_layout/csr64_probe"))
    (is (= "" (str warnings)))
    (with-artifact (test-source)
                   (fn [_ mapped]
                     (is (= 35.0 ((resolve 'sparse-layout.csr64-probe/lane-0-sum) mapped)))
                     (is (= 120.0 ((resolve 'sparse-layout.csr64-probe/lane-sum) mapped)))
                     (is (= 3 ((resolve 'sparse-layout.csr64-probe/row-col-sum) mapped 1 3)))))))

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
        (is (identical? point (overlay/copy-point! view 0 1 point 0)))
        (is (= [40.0 41.0 42.0] (vec point)))
        (is (identical? point (overlay/copy-point! view 0 0 point 0)))
        (is (= [1.0 2.0 3.0] (vec point)))
        (is (nil? (overlay/copy-point! view 1 0 point 0)))
        (is (identical? point (overlay/copy-point! view 1 1 point 0)))
        (is (= [70.0 71.0 72.0] (vec point)))
        (is (nil? (overlay/copy-point! view 2 1 point 0)))
        (is (identical? point (overlay/copy-point! view 3 0 point 0)))
        (is (= [90.0 91.0 92.0] (vec point)))
        (is (identical? point (overlay/copy-point! view 3 1 point 0)))
        (is (= [13.0 14.0 15.0] (vec point)))
        (testing "an absent coordinate in a row without structural patches"
          (is (nil? (overlay/copy-point! (overlay/compile-ledger mapped []) 1 1 point 0))))))))

(deftest ledger-overlay-copy-point-copies-to-offset-zero
  (with-artifact
    (test-source)
    (fn [_ mapped]
      (doseq [view
              [(overlay/compile-ledger mapped (ledger-fixture (double-array [40.0 41.0 42.0])))
               (overlay/compile-ledger mapped [])]

              row-id
              (range 4)

              col-id
              (range 2)]

        (is (= (point-copy 3 #(overlay/copy-point! view row-id col-id % 0))
               (point-copy 3 #(overlay/copy-point! view row-id col-id %))))))))

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
