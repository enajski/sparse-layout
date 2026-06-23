(ns sparse-layout.csr-source-test
  (:require [clojure.test :refer [deftest is]]
            [sparse-layout.core :refer [defsparse]]
            [sparse-layout.csr-source :as csr])
  (:import [java.nio ByteBuffer ByteOrder]
           [java.nio.file Files Path StandardOpenOption]))

(defsparse query-features
  {:row-key [:row]
   :cols-path [:vals]
   :payload {:kind :fixed-double-block
             :dim 3}
   :indices #{:csr}})

(defsparse scalar-features
  {:row-key [:row]
   :cols-path [:vals]
   :payload :double
   :indices #{:csr}})

(defsparse typed-key-features
  {:row-key [:row]
   :cols-path [:vals]
   :payload {:kind :fixed-double-block
             :dim 2}
   :indices #{:csr}})

(deftype FieldOnlyDataset
  [rowToId
   idToRow
   colToId
   idToCol
   edgeRows
   edgeCols
   csrRowPtrs
   csrColIds
   cscColPtrs
   cscRowIds
   cscPayloadIds
   payloadKind
   payloadDim
   payloadValues
   payloadPtrs])

(defn- test-dataset []
  (query-features-compile
   [{:row [:tenant-a :portfolio-1 :book-1]
     :vals (array-map :f1 [1.0 2.0 3.0]
                      :f2 [4.0 5.0 6.0])}
    {:row [:tenant-a :portfolio-1 :book-2]
     :vals (array-map :f1 [7.0 8.0 9.0])}
    {:row [:tenant-a :portfolio-2 :book-1]
     :vals (array-map :f1 [10.0 11.0 12.0])}
    {:row [:tenant-b :portfolio-1 :book-1]
     :vals (array-map :f2 [13.0 14.0 15.0])}]))

(defn- copied-block [source entry-id]
  (let [out (double-array (csr/csr-block-dim source))]
    (csr/csr-copy-block! source entry-id out 0)
    (vec (seq out))))

(defn- collect-source-row [source row-id]
  (let [seen (atom [])]
    (csr/csr-scan-row! source row-id
                       (fn [row-id* col-id entry-id src]
                         (swap! seen conj {:row-id row-id*
                                           :col-id col-id
                                           :col-key (csr/csr-col-key-at src col-id)
                                           :entry-id entry-id
                                           :block (copied-block src entry-id)})))
    @seen))

(defn- collect-merged-row [source delta row-key]
  (let [seen (atom [])]
    (csr/scan-merged-row!
     source
     delta
     row-key
     (fn [row-id row-key* col-id col-key origin entry-id block]
       (swap! seen conj {:row-id row-id
                         :row-key row-key*
                         :col-id col-id
                         :col-key col-key
                         :origin origin
                         :entry-id entry-id
                         :block (if (= :main origin)
                                  (copied-block source entry-id)
                                  (vec (seq block)))})))
    @seen))

(defn- temp-artifact ^Path []
  (Files/createTempFile "sparse-layout-csr-" ".slcsr"
                        (make-array java.nio.file.attribute.FileAttribute 0)))

(defn- artifact-magic-bytes []
  (byte-array
   (concat (seq (.getBytes "SLCSR" "US-ASCII"))
           [(byte 0) (byte 1) (byte 0)])))

(defn- artifact-bytes [source]
  (let [path (temp-artifact)]
    (try
      (csr/write-csr-artifact! source path)
      (Files/readAllBytes path)
      (finally
        (Files/deleteIfExists path)))))

(defn- section-entry-offset [section-id]
  (+ 64 (* 32 (dec section-id))))

(defn- section-offset [^ByteBuffer buffer section-id]
  (.getLong buffer (+ (section-entry-offset section-id) 16)))

(defn- corrupt-artifact-path [source mutate!]
  (let [path (temp-artifact)
        bytes (artifact-bytes source)
        buffer (doto (ByteBuffer/wrap bytes)
                 (.order ByteOrder/LITTLE_ENDIAN))]
    (mutate! buffer)
    (Files/write path bytes (make-array StandardOpenOption 0))
    path))

(defn- roundtrip-source [source f]
  (let [path (temp-artifact)]
    (try
      (csr/write-csr-artifact! source path)
      (f path (csr/open-csr-artifact path))
      (finally
        (Files/deleteIfExists path)))))

(deftest adapts-fixed-double-block-dataset-to-heap-csr-source
  (let [source (csr/dataset->csr-source (test-dataset))]
    (is (= 4 (csr/csr-row-count source)))
    (is (= 2 (csr/csr-col-count source)))
    (is (= 5 (csr/csr-entry-count source)))
    (is (= 3 (csr/csr-block-dim source)))
    (is (= [:tenant-a :portfolio-1 :book-1]
           (csr/csr-row-key-at source 0)))
    (is (= [0 2] (csr/csr-row-span source 0)))
    (is (= [[:f1 [1.0 2.0 3.0]]
            [:f2 [4.0 5.0 6.0]]]
           (mapv (fn [{:keys [col-key block]}] [col-key block])
                 (collect-source-row source 0))))))

(deftest adapts-generated-field-shape-when-protocol-identity-is-stale
  (let [ds (test-dataset)
        internals (sparse-layout.core/sparse-internals ds)
        field-only (->FieldOnlyDataset (:row->id internals)
                                       (:id->row internals)
                                       (:col->id internals)
                                       (:id->col internals)
                                       (:edge-rows internals)
                                       (:edge-cols internals)
                                       (:csr-row-ptrs internals)
                                       (:csr-col-ids internals)
                                       (:csc-col-ptrs internals)
                                       (:csc-row-ids internals)
                                       (:csc-payload-ids internals)
                                       (:payload-kind internals)
                                       (:payload-dim internals)
                                       (:payload-values internals)
                                       (:payload-ptrs internals))
        source (csr/dataset->csr-source field-only)]
    (is (= 4 (csr/csr-row-count source)))
    (is (= 5 (csr/csr-entry-count source)))
    (is (= [:tenant-a :portfolio-1 :book-1]
           (csr/csr-row-key-at source 0)))
    (is (= [1.0 2.0 3.0] (copied-block source 0)))))

(deftest copies-blocks-into-caller-owned-buffer
  (let [source (csr/dataset->csr-source (test-dataset))
        out (double-array 5)]
    (csr/csr-copy-block! source 1 out 2)
    (is (= [0.0 0.0 4.0 5.0 6.0]
           (vec (seq out))))))

(deftest writes-language-neutral-artifact-header
  (let [bytes (artifact-bytes (csr/dataset->csr-source (test-dataset)))]
    (is (= (concat (mapv #(bit-and (int %) 0xff) (artifact-magic-bytes))
                   [0x01 0x00 0x00 0x00
                    0x04 0x03 0x02 0x01])
           (mapv #(bit-and (int %) 0xff) (take 16 bytes))))))

(deftest roundtrips-heap-source-through-mmap-artifact
  (let [heap (csr/dataset->csr-source (test-dataset))]
    (roundtrip-source
     heap
     (fn [_path mmap]
       (is (= 4 (csr/csr-row-count mmap)))
       (is (= 2 (csr/csr-col-count mmap)))
       (is (= 5 (csr/csr-entry-count mmap)))
       (is (= 3 (csr/csr-block-dim mmap)))
       (is (= (csr/csr-row-key-at heap 0)
              (csr/csr-row-key-at mmap 0)))
       (is (= (csr/csr-row-id heap [:tenant-a :portfolio-1 :book-1])
              (csr/csr-row-id mmap [:tenant-a :portfolio-1 :book-1])))
       (is (= (csr/csr-col-id heap :f2)
              (csr/csr-col-id mmap :f2)))
       (is (= (csr/csr-row-span heap 0)
              (csr/csr-row-span mmap 0)))
       (is (= (collect-source-row heap 0)
              (collect-source-row mmap 0)))
       (is (= {:row-count 4
               :col-count 2
               :entry-count 5
               :block-dim 3}
              (select-keys (csr/csr-artifact-metadata mmap)
                           [:row-count :col-count :entry-count :block-dim])))
       (is (= [{:row-start 0 :row-end 2}]
              (csr/csr-resolve-ranges
               (csr/with-range-index mmap identity)
               {:prefix [:tenant-a :portfolio-1]})))))))

(deftest roundtrips-typed-business-keys
  (let [row-key [nil false true 42 3.25 "desk" :tenant/acme]
        col-key [:factor 7]
        heap (csr/dataset->csr-source
              (typed-key-features-compile
               [{:row row-key
                 :vals (array-map "exposure" [1.0 2.0]
                                  :risk [3.0 4.0]
                                  col-key [5.0 6.0])}]))]
    (roundtrip-source
     heap
     (fn [_ mmap]
       (is (= row-key (csr/csr-row-key-at mmap 0)))
       (is (= 0 (csr/csr-row-id mmap row-key)))
       (is (= "exposure" (csr/csr-col-key-at mmap 0)))
       (is (= col-key (csr/csr-col-key-at mmap 2)))
       (is (= 2 (csr/csr-col-id mmap col-key)))
       (is (= [["exposure" [1.0 2.0]]
               [:risk [3.0 4.0]]
               [col-key [5.0 6.0]]]
              (mapv (fn [{:keys [col-key block]}] [col-key block])
                    (collect-source-row mmap 0))))))))

(deftest roundtrips-empty-sources-through-mmap-artifact
  (let [heap (csr/dataset->csr-source (query-features-compile []))]
    (roundtrip-source
     heap
     (fn [_ mmap]
       (is (= 0 (csr/csr-row-count mmap)))
       (is (= 0 (csr/csr-col-count mmap)))
       (is (= 0 (csr/csr-entry-count mmap)))
       (is (= 3 (csr/csr-block-dim mmap)))
       (is (= [{:row-start 0 :row-end 0}]
              (csr/csr-resolve-ranges mmap nil)))
       (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"out of bounds"
            (csr/csr-row-span mmap 0)))))))

(deftest resolves-query-shaped-prefix-ranges
  (let [source (-> (test-dataset)
                   csr/dataset->csr-source
                   (csr/with-range-index identity))]
    (is (= [{:row-start 0 :row-end 3}]
           (csr/csr-resolve-ranges source {:prefix [:tenant-a]})))
    (is (= [{:row-start 0 :row-end 2}]
           (csr/csr-resolve-ranges source {:prefix [:tenant-a :portfolio-1]})))
    (is (= [{:row-start 3 :row-end 4}]
           (csr/csr-resolve-ranges source {:prefix [:tenant-b]})))))

(deftest resolves-non-contiguous-prefix-ranges
  (let [source (-> (test-dataset)
                   csr/dataset->csr-source
                   (csr/with-range-index (fn [[_ _ book]]
                                           [book])))]
    (is (= [{:row-start 0 :row-end 1}
            {:row-start 2 :row-end 4}]
           (csr/csr-resolve-ranges source {:prefix [:book-1]})))))

(deftest delta-row-entries-are-deterministic
  (let [delta (csr/make-dok-delta 3)]
    (csr/delta-put! delta :r :z [1.0 2.0 3.0])
    (csr/delta-put! delta :r :a [4.0 5.0 6.0])
    (csr/delta-delete! delta :r :m)
    (is (= [:a :m :z]
           (mapv :col-key (csr/delta-row-entries delta :r))))))

(deftest merged-scan-applies-delta-over-main
  (let [source (csr/dataset->csr-source (test-dataset))
        delta (csr/make-dok-delta-for-source source)
        row-key [:tenant-a :portfolio-1 :book-1]]
    (csr/delta-put! delta row-key :f1 [100.0 101.0 102.0])
    (csr/delta-delete! delta row-key :f2)
    (csr/delta-put! delta row-key :f3 [200.0 201.0 202.0])
    (is (= [[:f1 :delta [100.0 101.0 102.0]]
            [:f3 :delta [200.0 201.0 202.0]]]
           (mapv (fn [{:keys [col-key origin block]}]
                   [col-key origin block])
                 (collect-merged-row source delta row-key))))))

(deftest merged-range-scan-is-deterministic-by-column
  (let [source (-> (test-dataset)
                   csr/dataset->csr-source
                   (csr/with-range-index identity))
        delta (csr/make-dok-delta-for-source source)
        seen (atom [])]
    (csr/delta-put! delta [:tenant-a :portfolio-1 :book-2] :f2 [20.0 21.0 22.0])
    (csr/scan-merged-ranges!
     source
     delta
     (csr/csr-resolve-ranges source {:prefix [:tenant-a :portfolio-1]})
     (fn [_ row-key _ col-key origin _ _]
       (swap! seen conj [row-key col-key origin])))
    (is (= [[[:tenant-a :portfolio-1 :book-1] :f1 :main]
            [[:tenant-a :portfolio-1 :book-1] :f2 :main]
            [[:tenant-a :portfolio-1 :book-2] :f1 :main]
            [[:tenant-a :portfolio-1 :book-2] :f2 :delta]]
           @seen))))

(deftest merged-row-id-scan-matches-row-key-scan
  (let [source (csr/dataset->csr-source (test-dataset))
        delta (csr/make-dok-delta-for-source source)
        row-key [:tenant-a :portfolio-1 :book-1]
        by-id (atom [])]
    (csr/delta-put! delta row-key :f2 [30.0 31.0 32.0])
    (csr/scan-merged-row-by-id!
     source
     delta
     (csr/csr-row-id source row-key)
     (fn [_ _ _ col-key origin _ _]
       (swap! by-id conj [col-key origin])))
    (is (= (mapv (fn [{:keys [col-key origin]}]
                   [col-key origin])
                 (collect-merged-row source delta row-key))
           @by-id))))

(deftest merged-scan-includes-delta-only-rows
  (let [source (csr/dataset->csr-source (test-dataset))
        delta (csr/make-dok-delta-for-source source)
        row-key [:tenant-c :portfolio-9 :book-9]]
    (csr/delta-put! delta row-key :f1 [1.0 1.5 2.0])
    (is (= [[-1 row-key :f1 :delta [1.0 1.5 2.0]]]
           (mapv (fn [{:keys [row-id row-key col-key origin block]}]
                   [row-id row-key col-key origin block])
                 (collect-merged-row source delta row-key))))))

(deftest merged-scans-work-against-mmap-sources
  (let [heap (csr/dataset->csr-source (test-dataset))]
    (roundtrip-source
     heap
     (fn [_ mmap]
       (let [delta (csr/make-dok-delta-for-source mmap)
             row-key [:tenant-a :portfolio-1 :book-1]]
         (csr/delta-put! delta row-key :f1 [100.0 101.0 102.0])
         (csr/delta-delete! delta row-key :f2)
         (csr/delta-put! delta row-key :f3 [200.0 201.0 202.0])
         (is (= [[:f1 :delta [100.0 101.0 102.0]]
                 [:f3 :delta [200.0 201.0 202.0]]]
                (mapv (fn [{:keys [col-key origin block]}]
                        [col-key origin block])
                      (collect-merged-row mmap delta row-key)))))))))

(deftest delta-put-copies-input-block
  (let [delta (csr/make-dok-delta 3)
        block (double-array [1.0 2.0 3.0])]
    (csr/delta-put! delta :r :c block)
    (aset block 0 99.0)
    (is (= [1.0 2.0 3.0]
           (vec (seq (:block (csr/delta-entry delta :r :c))))))))

(deftest dimensioned-deltas-reject-wrong-sized-blocks
  (let [delta (csr/make-dok-delta 3)]
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"wrong dimension"
         (csr/delta-put! delta :r :c [1.0])))))

(deftest sources-reject-out-of-bounds-ids
  (let [heap (csr/dataset->csr-source (test-dataset))]
    (doseq [source [heap]]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"out of bounds"
           (csr/csr-row-span source -1)))
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"out of bounds"
           (csr/csr-entry-col-id source (csr/csr-entry-count source)))))
    (roundtrip-source
     heap
     (fn [_ mmap]
       (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"out of bounds"
            (csr/csr-row-span mmap -1)))
       (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"out of bounds"
            (csr/csr-row-span mmap 9)))
       (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"out of bounds"
            (csr/csr-entry-col-id mmap 9)))))))

(deftest rejects-unsupported-payload-kinds
  (let [ds (scalar-features-compile
            [{:row :a :vals (array-map :x 1.0)}])]
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"fixed double block"
         (csr/dataset->csr-source ds)))))

(deftest rejects-invalid-artifact-magic
  (let [path (temp-artifact)]
    (try
      (Files/write path (byte-array 64) (make-array StandardOpenOption 0))
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"magic"
           (csr/open-csr-artifact path)))
      (finally
        (Files/deleteIfExists path)))))

(deftest rejects-artifacts-missing-required-sections
  (let [path (temp-artifact)
        buffer (doto (ByteBuffer/allocate 64)
                 (.order ByteOrder/LITTLE_ENDIAN)
                 (.put (artifact-magic-bytes))
                 (.putInt 1)
                 (.putInt 0x01020304)
                 (.putInt 64)
                 (.putInt 0)
                 (.putLong 64)
                 (.putLong 0)
                 (.putLong 0)
                 (.putLong 0)
                 (.putInt 1)
                 (.putInt 0)
                 (.flip))]
    (try
      (Files/write path (.array buffer) (make-array StandardOpenOption 0))
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"missing a required section"
           (csr/open-csr-artifact path)))
      (finally
        (Files/deleteIfExists path)))))

(deftest rejects-artifacts-with-duplicate-section-ids
  (let [source (csr/dataset->csr-source (test-dataset))
        path (corrupt-artifact-path
              source
              (fn [buffer]
                (.putInt buffer (section-entry-offset 2) 1)))]
    (try
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"duplicate section"
           (csr/open-csr-artifact path)))
      (finally
        (Files/deleteIfExists path)))))

(deftest rejects-artifacts-with-invalid-row-pointers
  (let [source (csr/dataset->csr-source (test-dataset))
        path (corrupt-artifact-path
              source
              (fn [buffer]
                (let [row-ptrs-offset (section-offset buffer 1)]
                  (.putInt buffer (+ row-ptrs-offset (* 4 4)) 4))))]
    (try
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"final row pointer"
           (csr/open-csr-artifact path)))
      (finally
        (Files/deleteIfExists path)))))

(deftest rejects-artifacts-with-out-of-bounds-column-ids
  (let [source (csr/dataset->csr-source (test-dataset))
        path (corrupt-artifact-path
              source
              (fn [buffer]
                (let [col-ids-offset (section-offset buffer 2)]
                  (.putInt buffer col-ids-offset 99))))]
    (try
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"column id is out of bounds"
           (csr/open-csr-artifact path)))
      (finally
        (Files/deleteIfExists path)))))
