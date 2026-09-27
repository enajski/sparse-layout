(ns sparse-layout.csr64-overlay
  "Immutable CSR64 overlay compiled from an ordered numeric modification ledger."
  (:require [sparse-layout.csr-source.protocols :as p]
            [sparse-layout.csr64 :as csr64])
  (:import [java.util Arrays HashMap]))

(set! *warn-on-reflection* true)

(def ^:private double-array-class (class (double-array 0)))

(deftype StructuralRowPatch [^long rowId entries ^long adjustment])

(deftype CSR64LedgerOverlay [base ^long throughSequence ^long ledgerRecordCount
                             ^long retainedModificationCount ^ints structuralRows
                             ^objects structuralPatches ^longs adjustments ^longs valueEntryIds
                             ^objects valueBlocks])

(defn- fail! [message data] (throw (ex-info message data)))

(defn- checked-add
  ^long [^long left ^long right]
  (try (Math/addExact left right)
       (catch ArithmeticException _
         (fail! "CSR64 overlay size overflow." {:left left :right right}))))

(defn- checked-mul
  ^long [^long left ^long right]
  (try (Math/multiplyExact left right)
       (catch ArithmeticException _
         (fail! "CSR64 overlay size overflow." {:left left :right right}))))

(defn- id!
  ^long [kind value upper sequence-id]
  (when-not (integer? value)
    (fail! "CSR64 ledger ids must be integers." {:kind kind :value value :sequence sequence-id}))
  (let [value (long value)]
    (when-not (<= 0 value (dec (long upper)))
      (fail! "CSR64 ledger id is out of bounds."
             {:kind kind :value value :upper-bound upper :sequence sequence-id}))
    (when (> value Integer/MAX_VALUE)
      (fail! "CSR64 ledger id does not fit the caller's int destination."
             {:kind kind :value value :sequence sequence-id}))
    value))

(defn- copy-block
  ^doubles [block block-dim sequence-id]
  (when-not (instance? double-array-class block)
    (fail! "CSR64 ledger put blocks must be double arrays."
           {:sequence sequence-id
            :class (some-> block
                           class
                           str)}))
  (let [block ^doubles block]
    (when-not (= (long block-dim) (alength block))
      (fail! "CSR64 ledger put block has the wrong dimension."
             {:sequence sequence-id :expected block-dim :actual (alength block)}))
    (Arrays/copyOf block (alength block))))

(defn- coordinate-key
  ^long [^long row-id ^long col-id]
  (bit-or (bit-shift-left row-id 32) (bit-and col-id 0xffffffff)))

(defn- replay-ledger
  [source modifications]
  (let [row-count
        (p/csr-row-count source)

        col-count
        (p/csr-col-count source)

        block-dim
        (p/csr-block-dim source)

        latest
        (HashMap.)]

    (loop [remaining
           (seq modifications)

           previous-sequence
           (long -1)

           record-count
           (long 0)]

      (if-let [record (first remaining)]
        (let [sequence-id (:sequence record)]
          (when-not (integer? sequence-id)
            (fail! "CSR64 ledger sequence must be an integer." {:record record}))
          (let [sequence-id (long sequence-id)]
            (when-not (> sequence-id previous-sequence)
              (fail! "CSR64 ledger sequence must be strictly increasing."
                     {:previous previous-sequence :actual sequence-id}))
            (let [op (:op record)
                  row-id (id! :row-id (:row-id record) row-count sequence-id)
                  col-id (id! :col-id (:col-id record) col-count sequence-id)
                  block (case op
                          :put
                          (copy-block (:block record) block-dim sequence-id)

                          :delete
                          nil

                          (fail! "CSR64 ledger operation must be :put or :delete."
                                 {:sequence sequence-id :op op}))
                  normalized
                  {:sequence sequence-id :op op :row-id row-id :col-id col-id :block block}]

              (.put latest (coordinate-key row-id col-id) normalized)
              (recur (next remaining) sequence-id (inc record-count)))))
        {:through-sequence previous-sequence
         :record-count record-count
         :latest (sort-by (juxt :row-id :col-id) (vec (.values latest)))}))))

(defn- effective-row
  [source entries]
  (let [entries
        (into []
              (keep (fn [{:keys [op row-id col-id] :as entry}]
                      (let [base-entry-id (csr64/find-entry source row-id col-id)]
                        (when-not (and (= :delete op) (= -1 base-entry-id))
                          (assoc entry :base-entry-id base-entry-id)))))
              entries)

        structural?
        (boolean (some (fn [{:keys [op base-entry-id]}]
                         (or (and (= :put op) (= -1 base-entry-id))
                             (and (= :delete op) (not= -1 base-entry-id))))
                       entries))

        adjustment
        (reduce (fn [total {:keys [op base-entry-id]}]
                  (+ total
                     (cond (and (= :put op) (= -1 base-entry-id)) 1
                           (= :delete op) -1
                           :else 0)))
                0
                entries)]

    {:entries entries :structural? structural? :adjustment adjustment}))

(defn compile-ledger
  "Compiles ordered numeric ledger records into an immutable CSR64 overlay.

  Records are maps with `:sequence`, `:op`, `:row-id`, and `:col-id`. `:put`
  records also carry a `double[]` `:block`. Sequence numbers must increase
  strictly; the latest record for a coordinate wins. The base source must
  outlive every compiled overlay derived from it."
  [source modifications]
  (when-not (instance? sparse_layout.csr64.CSR64Source source)
    (fail! "CSR64 ledger overlays require a CSR64Source."
           {:source-class (some-> source
                                  class
                                  str)}))
  (let [{:keys [through-sequence record-count latest]}
        (replay-ledger source modifications)

        rows
        (partition-by :row-id latest)

        compiled
        (into []
              (keep (fn [entries]
                      (let [{:keys [entries] :as row} (effective-row source entries)]
                        (when (seq entries) (assoc row :row-id (:row-id (first entries)))))))
              rows)

        structural
        (filterv :structural? compiled)

        replacements
        (->> compiled
             (remove :structural?)
             (mapcat :entries)
             (sort-by :base-entry-id)
             vec)

        structural-count
        (count structural)

        structural-modification-count
        (reduce + (map #(count (:entries %)) structural))

        replacement-count
        (count replacements)

        structural-rows
        (int-array structural-count)

        structural-patches
        (object-array structural-count)

        adjustments
        (long-array (inc structural-count))

        value-entry-ids
        (long-array replacement-count)

        value-blocks
        (object-array replacement-count)]

    (dotimes [idx structural-count]
      (let [{:keys [row-id entries adjustment]} (nth structural idx)]
        (aset structural-rows idx (int row-id))
        (aset structural-patches idx (->StructuralRowPatch row-id entries adjustment))
        (aset adjustments (inc idx) (+ (aget adjustments idx) (long adjustment)))))
    (dotimes [idx replacement-count]
      (let [{:keys [base-entry-id block]} (nth replacements idx)]
        (aset value-entry-ids idx (long base-entry-id))
        (aset value-blocks idx block)))
    (->CSR64LedgerOverlay source
                          through-sequence
                          record-count
                          (+ structural-modification-count replacement-count)
                          structural-rows
                          structural-patches
                          adjustments
                          value-entry-ids
                          value-blocks)))

(defn metadata
  "Returns immutable ledger-cut and overlay-size metadata."
  [^CSR64LedgerOverlay overlay]
  (let [structural-rows
        ^ints (.-structuralRows overlay)

        value-entry-ids
        ^longs (.-valueEntryIds overlay)

        adjustments
        ^longs (.-adjustments overlay)]

    {:through-sequence (.-throughSequence overlay)
     :ledger-record-count (.-ledgerRecordCount overlay)
     :retained-modification-count (.-retainedModificationCount overlay)
     :structural-row-count (alength structural-rows)
     :replacement-count (alength value-entry-ids)
     :net-entry-adjustment (aget adjustments (dec (alength adjustments)))}))

(defn- lower-bound-int
  ^long [^ints values ^long target]
  (loop [low
         (long 0)

         high
         (long (alength values))]

    (if (< low high)
      (let [middle (quot (+ low high) 2)]
        (if (< (aget values (int middle)) target) (recur (inc middle) high) (recur low middle)))
      low)))

(defn- lower-bound-long
  ^long [^longs values ^long target]
  (loop [low
         (long 0)

         high
         (long (alength values))]

    (if (< low high)
      (let [middle (quot (+ low high) 2)]
        (if (< (aget values (int middle)) target) (recur (inc middle) high) (recur low middle)))
      low)))

(defn- structural-index
  ^long [^CSR64LedgerOverlay overlay ^long row-id]
  (let [rows
        ^ints (.-structuralRows overlay)

        idx
        (lower-bound-int rows row-id)]

    (if (and (< idx (alength rows)) (= row-id (aget rows (int idx)))) idx -1)))

(defn- base-prefix
  ^long [source ^long row-id]
  (if (= row-id (p/csr-row-count source))
    (p/csr-entry-count source)
    (long (first (p/csr-row-span source row-id)))))

(defn- visible-prefix
  ^long [^CSR64LedgerOverlay overlay ^long row-id]
  (let [rows
        ^ints (.-structuralRows overlay)

        adjustments
        ^longs (.-adjustments overlay)

        idx
        (lower-bound-int rows row-id)]

    (+ (base-prefix (.-base overlay) row-id) (aget adjustments (int idx)))))

(defn- ensure-range!
  [source row-start row-end]
  (when-not (and (integer? row-start) (integer? row-end))
    (fail! "CSR64 overlay row range must use integer ids." {:row-start row-start :row-end row-end}))
  (let [row-start
        (long row-start)

        row-end
        (long row-end)

        row-count
        (p/csr-row-count source)]

    (when-not (<= 0 row-start row-end row-count)
      (fail! "CSR64 overlay row range is out of bounds."
             {:row-start row-start :row-end row-end :row-count row-count})))
  nil)

(defn range-entry-count
  "Returns the visible entry count in a half-open numeric row range."
  ^long [^CSR64LedgerOverlay overlay row-start row-end]
  (ensure-range! (.-base overlay) row-start row-end)
  (- (visible-prefix overlay (long row-end)) (visible-prefix overlay (long row-start))))

(defn- ensure-page!
  [^CSR64LedgerOverlay overlay row-start row-end cursor max-entries dst-entry-off ^ints dst-row-ids
   ^ints dst-col-ids ^doubles dst-values]
  (doseq [[kind value positive?] [[:cursor cursor false] [:max-entries max-entries true]
                                  [:dst-entry-off dst-entry-off false]]]
    (when-not (and (integer? value) (if positive? (pos? (long value)) (not (neg? (long value)))))
      (fail! "CSR64 overlay page argument is invalid." {:kind kind :value value})))
  (ensure-range! (.-base overlay) row-start row-end)
  (let [total
        (range-entry-count overlay row-start row-end)

        cursor
        (long cursor)]

    (when (> cursor total)
      (fail! "CSR64 overlay cursor exceeds the selected range."
             {:cursor cursor :entry-count total}))
    (let [copied
          (min (long max-entries) (- total cursor))

          entry-end
          (checked-add (long dst-entry-off) copied)

          value-end
          (checked-mul entry-end (p/csr-block-dim (.-base overlay)))]

      (when (or (> entry-end (alength dst-row-ids))
                (> entry-end (alength dst-col-ids))
                (> value-end (alength dst-values)))
        (fail! "CSR64 overlay page destination is out of bounds."
               {:copied copied
                :dst-entry-off dst-entry-off
                :row-capacity (alength dst-row-ids)
                :col-capacity (alength dst-col-ids)
                :value-capacity (alength dst-values)}))
      copied)))

(defn- row-for-position
  ^long [^CSR64LedgerOverlay overlay ^long row-start ^long row-end ^long position]
  (loop [low
         row-start

         high
         (dec row-end)]

    (if (< low high)
      (let [middle (quot (+ low high 1) 2)]
        (if (<= (visible-prefix overlay middle) position)
          (recur middle high)
          (recur low (dec middle))))
      low)))

(defn- apply-value-patches!
  [^CSR64LedgerOverlay overlay entry-start copied ^doubles dst-values dst-entry-off]
  (let [entry-start
        (long entry-start)

        copied
        (long copied)

        dst-entry-off
        (long dst-entry-off)

        entry-ids
        ^longs (.-valueEntryIds overlay)

        blocks
        ^objects (.-valueBlocks overlay)

        entry-end
        (+ entry-start copied)

        block-dim
        (p/csr-block-dim (.-base overlay))]

    (loop [idx (lower-bound-long entry-ids entry-start)]
      (when (and (< idx (alength entry-ids)) (< (aget entry-ids (int idx)) entry-end))
        (let [entry-id (aget entry-ids (int idx))
              block ^doubles (aget blocks (int idx))
              out-entry (+ dst-entry-off (- entry-id entry-start))]

          (System/arraycopy block 0 dst-values (int (* out-entry block-dim)) (int block-dim))
          (recur (inc idx))))))
  nil)

(defn- write-main!
  [source row-id entry-id ^ints dst-row-ids ^ints dst-col-ids ^doubles dst-values dst-entry-off]
  (let [row-id
        (long row-id)

        entry-id
        (long entry-id)

        dst-entry-off
        (long dst-entry-off)]

    (aset dst-row-ids (int dst-entry-off) (int row-id))
    (aset dst-col-ids (int dst-entry-off) (int (p/csr-entry-col-id source entry-id)))
    (p/csr-copy-block! source entry-id dst-values (* dst-entry-off (p/csr-block-dim source)))))

(defn- write-patch!
  [source row-id entry ^ints dst-row-ids ^ints dst-col-ids ^doubles dst-values dst-entry-off]
  (let [row-id
        (long row-id)

        dst-entry-off
        (long dst-entry-off)]

    (aset dst-row-ids (int dst-entry-off) (int row-id))
    (aset dst-col-ids (int dst-entry-off) (int (:col-id entry)))
    (System/arraycopy ^doubles (:block entry)
                      0
                      dst-values
                      (int (* dst-entry-off (p/csr-block-dim source)))
                      (int (p/csr-block-dim source)))))

(defn- copy-structural-row!
  [source ^StructuralRowPatch patch row-offset limit ^ints dst-row-ids ^ints dst-col-ids
   ^doubles dst-values dst-entry-off]
  (let [row-offset
        (long row-offset)

        limit
        (long limit)

        dst-entry-off
        (long dst-entry-off)

        [main-start main-end]
        (p/csr-row-span source (.-rowId patch))

        entries
        (.-entries patch)

        patch-count
        (count entries)]

    (loop [main-entry
           (long main-start)

           patch-idx
           (long 0)

           position
           (long 0)

           written
           (long 0)]

      (if (or (= written limit) (and (= main-entry main-end) (= patch-idx patch-count)))
        written
        (cond (= main-entry main-end)
              (let [{:keys [op] :as entry} (nth entries patch-idx)]
                (if (= :put op)
                  (if (< position row-offset)
                    (recur main-entry (inc patch-idx) (inc position) written)
                    (do (write-patch! source
                                      (.-rowId patch)
                                      entry
                                      dst-row-ids
                                      dst-col-ids
                                      dst-values
                                      (+ dst-entry-off written))
                        (recur main-entry (inc patch-idx) (inc position) (inc written))))
                  (recur main-entry (inc patch-idx) position written)))
              (= patch-idx patch-count)
              (if (< position row-offset)
                (recur (inc main-entry) patch-idx (inc position) written)
                (do (write-main! source
                                 (.-rowId patch)
                                 main-entry
                                 dst-row-ids
                                 dst-col-ids
                                 dst-values
                                 (+ dst-entry-off written))
                    (recur (inc main-entry) patch-idx (inc position) (inc written))))
              :else
              (let [main-col-id
                    (p/csr-entry-col-id source main-entry)

                    {:keys [op col-id] :as entry}
                    (nth entries patch-idx)]

                (cond (< main-col-id col-id)
                      (if (< position row-offset)
                        (recur (inc main-entry) patch-idx (inc position) written)
                        (do (write-main! source
                                         (.-rowId patch)
                                         main-entry
                                         dst-row-ids
                                         dst-col-ids
                                         dst-values
                                         (+ dst-entry-off written))
                            (recur (inc main-entry) patch-idx (inc position) (inc written))))
                      (> main-col-id col-id)
                      (if (= :put op)
                        (if (< position row-offset)
                          (recur main-entry (inc patch-idx) (inc position) written)
                          (do (write-patch! source
                                            (.-rowId patch)
                                            entry
                                            dst-row-ids
                                            dst-col-ids
                                            dst-values
                                            (+ dst-entry-off written))
                              (recur main-entry (inc patch-idx) (inc position) (inc written))))
                        (recur main-entry (inc patch-idx) position written))
                      (= :put op)
                      (if (< position row-offset)
                        (recur (inc main-entry) (inc patch-idx) (inc position) written)
                        (do (write-patch! source
                                          (.-rowId patch)
                                          entry
                                          dst-row-ids
                                          dst-col-ids
                                          dst-values
                                          (+ dst-entry-off written))
                            (recur (inc main-entry) (inc patch-idx) (inc position) (inc written))))
                      :else (recur (inc main-entry) (inc patch-idx) position written))))))))

(defn copy-page!
  "Copies a bounded visible page from the base plus one compiled ledger cut."
  [^CSR64LedgerOverlay overlay row-start row-end cursor max-entries ^ints dst-row-ids
   ^ints dst-col-ids ^doubles dst-values dst-entry-off]
  (let [copied (ensure-page! overlay
                             row-start
                             row-end
                             cursor
                             max-entries
                             dst-entry-off
                             dst-row-ids
                             dst-col-ids
                             dst-values)]
    (if (zero? copied)
      0
      (let [row-start (long row-start)
            row-end (long row-end)
            absolute-position (+ (visible-prefix overlay row-start) (long cursor))
            first-row (row-for-position overlay row-start row-end absolute-position)
            first-row-offset (- absolute-position (visible-prefix overlay first-row))
            source (.-base overlay)
            structural-rows ^ints (.-structuralRows overlay)
            structural-patches ^objects (.-structuralPatches overlay)]

        (loop [row-id first-row
               row-offset first-row-offset
               remaining copied
               out (long dst-entry-off)]

          (if (zero? remaining)
            copied
            (let [structural-idx (structural-index overlay row-id)]
              (if (not= -1 structural-idx)
                (let [patch ^StructuralRowPatch (aget structural-patches (int structural-idx))
                      row-count (- (visible-prefix overlay (inc row-id))
                                   (visible-prefix overlay row-id))
                      requested (min remaining (- row-count row-offset))
                      written (long (copy-structural-row! source
                                                          patch
                                                          row-offset
                                                          requested
                                                          dst-row-ids
                                                          dst-col-ids
                                                          dst-values
                                                          out))]

                  (recur (inc row-id) 0 (- remaining written) (+ out written)))
                (let [next-idx (lower-bound-int structural-rows row-id)
                      clean-end (long (if (< next-idx (alength structural-rows))
                                        (min row-end (aget structural-rows (int next-idx)))
                                        row-end))
                      base-count (csr64/range-entry-count source row-id clean-end)
                      available (- base-count row-offset)]

                  (if (zero? available)
                    (recur clean-end 0 remaining out)
                    (let [requested (min remaining available)
                          entry-start (+ (base-prefix source row-id) row-offset)
                          written (long (csr64/copy-page! source
                                                          row-id
                                                          clean-end
                                                          row-offset
                                                          requested
                                                          dst-row-ids
                                                          dst-col-ids
                                                          dst-values
                                                          out))]

                      (apply-value-patches! overlay entry-start written dst-values out)
                      (recur (long (if (= written available) clean-end row-id))
                             (if (= written available) 0 (+ row-offset written))
                             (- remaining written)
                             (+ out written)))))))))))))

(defn- patch-entry
  [^StructuralRowPatch patch ^long col-id]
  (let [entries (.-entries patch)]
    (loop [low (long 0)
           high (long (count entries))]

      (if (< low high)
        (let [middle (quot (+ low high) 2)
              entry (nth entries middle)
              actual (long (:col-id entry))]

          (cond (< actual col-id) (recur (inc middle) high)
                (> actual col-id) (recur low middle)
                :else entry))
        nil))))

(defn- ensure-point-destination!
  [source dst dst-off]
  (when-not (and (integer? dst-off) (not (neg? (long dst-off))))
    (fail! "CSR64 overlay destination offset is invalid." {:dst-off dst-off}))
  (let [end (checked-add (long dst-off) (p/csr-block-dim source))]
    (when (> end (alength ^doubles dst))
      (fail!
        "CSR64 overlay block destination is out of bounds."
        {:dst-off dst-off :block-dim (p/csr-block-dim source) :capacity (alength ^doubles dst)}))))

(defn copy-point!
  "Copies the visible block for one numeric coordinate; returns true when present."
  [^CSR64LedgerOverlay overlay row-id col-id ^doubles dst dst-off]
  (let [source
        (.-base overlay)

        row-id
        (id! :row-id row-id (p/csr-row-count source) nil)

        col-id
        (id! :col-id col-id (p/csr-col-count source) nil)

        structural-patches
        ^objects (.-structuralPatches overlay)

        value-entry-ids
        ^longs (.-valueEntryIds overlay)

        value-blocks
        ^objects (.-valueBlocks overlay)]

    (ensure-point-destination! source dst dst-off)
    (let [structural-idx (structural-index overlay row-id)]
      (if (not= -1 structural-idx)
        (if-let [{:keys [op block]} (patch-entry (aget structural-patches (int structural-idx))
                                                 col-id)]
          (when (= :put op)
            (System/arraycopy ^doubles block 0 dst (int dst-off) (int (p/csr-block-dim source)))
            true)
          (csr64/copy-point! source row-id col-id dst dst-off))
        (let [entry-id (csr64/find-entry source row-id col-id)]
          (if (= -1 entry-id)
            false
            (let [idx (lower-bound-long value-entry-ids entry-id)]
              (if (and (< idx (alength value-entry-ids))
                       (= entry-id (aget value-entry-ids (int idx))))
                (do (System/arraycopy ^doubles (aget value-blocks (int idx))
                                      0
                                      dst
                                      (int dst-off)
                                      (int (p/csr-block-dim source)))
                    true)
                (csr64/copy-point! source row-id col-id dst dst-off)))))))))
