(ns sparse-layout.bench-data
  (:require [sparse-layout.core :as sparse :refer [defsparse]]
            [sparse-layout.facade :as facade])
  (:import [mikera.vectorz Vector]))

(def block-dim 4)

(def benchmark-configs
  [{:label "scalar keys / small"
    :key-shape :scalar
    :scale :small
    :standard? true
    :row-count 2048
    :col-count 256
    :nnz-per-row 12}
   {:label "compound map keys / small"
    :key-shape :compound-map
    :scale :small
    :standard? true
    :row-count 2048
    :col-count 256
    :nnz-per-row 12}
   {:label "scalar keys / medium"
    :key-shape :scalar
    :scale :medium
    :row-count 16384
    :col-count 1024
    :nnz-per-row 16}
   {:label "compound map keys / medium"
    :key-shape :compound-map
    :scale :medium
    :row-count 16384
    :col-count 1024
    :nnz-per-row 16}
   {:label "scalar keys / large"
    :key-shape :scalar
    :scale :large
    :row-count 32768
    :col-count 2048
    :nnz-per-row 24}
   {:label "compound map keys / large"
    :key-shape :compound-map
    :scale :large
    :row-count 32768
    :col-count 2048
    :nnz-per-row 24}])

(defsparse bench-features
           {:row-key [:entity]
            :cols-path [:vals]
            :payload {:kind :fixed-double-block :dim block-dim}
            :indices #{:csr :csc}
            :retain-coo? false})

(defn- scalar-row-key [row-id] row-id)

(defn- scalar-col-key [col-id] (keyword (str "c" col-id)))

(defn- compound-row-key
  [row-id]
  {:tenant (str "tenant-" (mod row-id 17))
   :region (nth [:na :eu :apac :latam] (mod row-id 4))
   :bucket (quot row-id 1024)
   :entity row-id})

(defn- compound-col-key
  [col-id]
  {:family (str "family-" (mod col-id 13))
   :metric (keyword (str "m" col-id))
   :window-minutes (nth [1 5 15 60] (mod col-id 4))
   :stat (nth [:sum :avg :p95 :max] (mod col-id 4))})

(defn row-key-fn
  [{:keys [key-shape]}]
  (case key-shape
    :scalar
    scalar-row-key

    :compound-map
    compound-row-key))

(defn col-key-fn
  [{:keys [key-shape]}]
  (case key-shape
    :scalar
    scalar-col-key

    :compound-map
    compound-col-key))

(defn equivalent-query-key [key] (if (map? key) (into {} key) key))

(defn row-col-ids
  [row-id col-count nnz-per-row]
  (mapv #(mod (+ (* row-id 31) (* % 17)) col-count) (range nnz-per-row)))

(defn payload
  [row-id col-id]
  (mapv (fn [i]
          (double (+ (* row-id 0.001) (* col-id 0.01) i)))
        (range block-dim)))

(defn make-entries
  "Builds deterministic flat [row col payload] entries used as the common
  source for all construction benchmarks."
  [{:keys [row-count col-count nnz-per-row] :as config}]
  (let [row-key
        (row-key-fn config)

        col-key
        (col-key-fn config)

        rows
        (mapv row-key (range row-count))

        cols
        (mapv col-key (range col-count))]

    (into []
          (mapcat (fn [row-id]
                    (map (fn [col-id]
                           [(nth rows row-id) (nth cols col-id) (payload row-id col-id)])
                         (row-col-ids row-id col-count nnz-per-row))))
          (range row-count))))

(defn entries->sparse
  "Builds a sparse dataset directly from flat entries using the same mutable
  COO builder that generated ingest functions use."
  [entries]
  (let [builder (make-bench-features-builder)]
    (doseq [[row col payload] entries]
      (sparse/append-entry! builder row col payload))
    (bench-features-freeze! builder)))

(defn entries->nested-row-index
  [entries]
  (reduce (fn [rows [row col payload]]
            (update rows row assoc col payload))
          {}
          entries))

(defn entries->nested-dual-index
  [entries]
  (reduce (fn [{:keys [rows cols]} [row col payload]]
            {:rows (update rows row assoc col payload) :cols (update cols col assoc row payload)})
          {:rows {} :cols {}}
          entries))

(defn entries->nested-row-vectorz-index
  [entries]
  (reduce (fn [rows [row col payload]]
            (update rows row assoc col (Vector/of ^doubles (double-array payload))))
          {}
          entries))

(defn first-payload-value ^double [payload] (double (nth payload 0)))

(defn sparse-direct-point-value
  ^double [ds row col]
  (sparse/block-view-value (bench-features-block-view ds row col) 0))

(defn sparse-map-point-value
  ^double [sparse-map row col]
  (sparse/block-view-value (get-in sparse-map [row col]) 0))

(defn nested-row-point-value
  ^double [nested-rows row col]
  (first-payload-value (get-in nested-rows [row col])))

(defn nested-vectorz-point-value
  ^double [nested-rows row col]
  (.get ^Vector (get-in nested-rows [row col]) (int 0)))

(defn sparse-row-map-sum
  ^double [ds row]
  (reduce-kv (fn [sum _ view]
               (+ (double sum) (sparse/block-view-value view 0)))
             0.0
             (facade/row-view-map ds row)))

(defn nested-row-map-sum
  ^double [nested-rows row]
  (reduce-kv (fn [sum _ payload]
               (+ (double sum) (first-payload-value payload)))
             0.0
             (get nested-rows row)))

(defn nested-vectorz-row-map-sum
  ^double [nested-rows row]
  (reduce-kv (fn [^double sum _ ^Vector v]
               (+ sum (.get v (int 0))))
             0.0
             (get nested-rows row)))

(defn sparse-col-map-sum
  ^double [ds col]
  (reduce-kv (fn [sum _ view]
               (+ (double sum) (sparse/block-view-value view 0)))
             0.0
             (facade/col-view-map ds col)))

(defn nested-dual-col-map-sum
  ^double [nested-dual col]
  (reduce-kv (fn [sum _ payload]
               (+ (double sum) (first-payload-value payload)))
             0.0
             (get-in nested-dual [:cols col])))

(defn nested-row-only-col-sum
  ^double [nested-rows col]
  (reduce-kv (fn [sum _ row-map]
               (if-let [payload (get row-map col)]
                 (+ (double sum) (first-payload-value payload))
                 sum))
             0.0
             nested-rows))

(defn prepare
  [{:keys [row-count col-count nnz-per-row] :as config}]
  (let [entries
        (make-entries config)

        sparse
        (entries->sparse entries)

        sparse-map
        (facade/as-view-map sparse)

        nested-rows
        (entries->nested-row-index entries)

        nested-dual
        (entries->nested-dual-index entries)

        nested-vectorz
        (entries->nested-row-vectorz-index entries)

        hot-row-id
        (quot row-count 2)

        hot-col-id
        (first (row-col-ids hot-row-id col-count nnz-per-row))

        row-key
        ((row-key-fn config) hot-row-id)

        col-key
        ((col-key-fn config) hot-col-id)

        scan-col
        ((col-key-fn config) (quot col-count 3))]

    {:config config
     :entries entries
     :sparse sparse
     :sparse-map sparse-map
     :nested-rows nested-rows
     :nested-dual nested-dual
     :nested-vectorz nested-vectorz
     :row-key (equivalent-query-key row-key)
     :col-key (equivalent-query-key col-key)
     :scan-col (equivalent-query-key scan-col)}))

(defn nnz [{:keys [row-count nnz-per-row]}] (* row-count nnz-per-row))

(defn print-config
  [{:keys [label key-shape scale row-count col-count nnz-per-row] :as config}]
  (println)
  (println "===" label "===")
  (println "Config:"
           {:key-shape key-shape
            :scale scale
            :rows row-count
            :cols col-count
            :nnz-per-row nnz-per-row
            :nnz (nnz config)
            :block-dim block-dim}))

(defn standard-configs [] (filterv :standard? benchmark-configs))

(defn- selected-scales
  [args]
  (cond (some #{"all" "large"} args) #{:small :medium :large}
        (some #{"large-only"} args) #{:large}
        (some #{"medium"} args) #{:medium}
        (some #{"small"} args) #{:small}
        :else #{:small}))

(defn- selected-key-shapes
  [args]
  (cond (some #{"scalar"} args) #{:scalar}
        (some #{"compound"} args) #{:compound-map}
        :else #{:scalar :compound-map}))

(defn selected-configs
  [args]
  (let [scales
        (selected-scales args)

        key-shapes
        (selected-key-shapes args)]

    (filterv #(and (contains? scales (:scale %)) (contains? key-shapes (:key-shape %)))
      benchmark-configs)))
