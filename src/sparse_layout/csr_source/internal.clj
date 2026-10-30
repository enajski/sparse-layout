(ns sparse-layout.csr-source.internal
  "Shared range normalization and validation for CSRSource storage backends.")

(defn row-range [start end] {:row-start (int start) :row-end (int end)})

(defn range-start
  ^long [r]
  (cond (map? r) (long (:row-start r))
        (vector? r) (long (nth r 0))
        (seq? r) (long (first r))
        :else (throw (ex-info "Unsupported CSR row range." {:range r}))))

(defn range-end
  ^long [r]
  (cond (map? r) (long (:row-end r))
        (vector? r) (long (nth r 1))
        (seq? r) (long (second r))
        :else (throw (ex-info "Unsupported CSR row range." {:range r}))))

(defn normalize-range [r] [(int (range-start r)) (int (range-end r))])

(defn normalize-prefix
  [prefix]
  (cond (nil? prefix) []
        (vector? prefix) prefix
        (sequential? prefix) (vec prefix)
        :else [prefix]))

(defn ensure-index!
  [label value upper-bound]
  (let [value
        (long value)

        upper-bound
        (long upper-bound)]

    (when (or (neg? value) (>= value upper-bound))
      (throw (ex-info "CSRSource index is out of bounds."
                      {:index label :value value :upper-bound upper-bound})))
    value))

(defn ensure-copy-target!
  [dst dst-off block-dim]
  (let [dst-len
        (alength ^doubles dst)

        dst-off
        (long dst-off)

        block-dim
        (long block-dim)]

    (when (or (neg? dst-off) (> (+ dst-off block-dim) dst-len))
      (throw (ex-info "CSRSource copy destination is out of bounds."
                      {:dst-off dst-off :block-dim block-dim :dst-length dst-len})))
    dst-off))

(defn ensure-row-range!
  [start end row-count]
  (let [start
        (long start)

        end
        (long end)

        row-count
        (long row-count)]

    (when (or (neg? start) (> start end) (> end row-count))
      (throw (ex-info "CSR row range is out of bounds."
                      {:row-start start :row-end end :row-count row-count}))))
  nil)

(defn ensure-ranges-copy-target!
  [dst-row-ids dst-col-ids dst-values dst-entry-off entry-count block-dim]
  (let [dst-row-ids
        ^ints dst-row-ids

        dst-col-ids
        ^ints dst-col-ids

        dst-values
        ^doubles dst-values

        dst-entry-off
        (long dst-entry-off)

        entry-count
        (long entry-count)

        block-dim
        (long block-dim)

        entry-end
        (+ dst-entry-off entry-count)

        value-end
        (* entry-end block-dim)]

    (when (or (neg? dst-entry-off)
              (> entry-end (alength dst-row-ids))
              (> entry-end (alength dst-col-ids))
              (> value-end (alength dst-values)))
      (throw (ex-info "CSR range copy destination is out of bounds."
                      {:dst-entry-off dst-entry-off
                       :entry-count entry-count
                       :block-dim block-dim
                       :row-id-capacity (alength dst-row-ids)
                       :col-id-capacity (alength dst-col-ids)
                       :value-capacity (alength dst-values)}))))
  nil)
