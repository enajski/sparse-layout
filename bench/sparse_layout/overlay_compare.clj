(ns sparse-layout.overlay-compare
  "Measures the DOK overlay as it grows over a 295-double CSR source."
  (:gen-class)
  (:require [sparse-layout.bench-data :as data]
            [sparse-layout.core :as sparse]
            [sparse-layout.csr-source :as csr])
  (:import [com.sun.management ThreadMXBean]
           [java.lang.management ManagementFactory]
           [java.util Arrays]))

(def block-dim 295)

(sparse/defsparse overlay-features
                  {:row-key [:entity]
                   :cols-path [:vals]
                   :payload {:kind :fixed-double-block :dim block-dim}
                   :indices #{:csr}
                   :retain-coo? false})

(set! *warn-on-reflection* true)

(def ^:private ^ThreadMXBean thread-mx-bean
  (let [bean ^ThreadMXBean (cast ThreadMXBean (ManagementFactory/getThreadMXBean))]
    (when (and (.isThreadAllocatedMemorySupported bean)
               (not (.isThreadAllocatedMemoryEnabled bean)))
      (.setThreadAllocatedMemoryEnabled bean true))
    bean))

(defn- allocated-bytes
  ^long []
  (.getThreadAllocatedBytes thread-mx-bean (.getId (Thread/currentThread))))

(defn- timed
  [f]
  (let [before
        (allocated-bytes)

        start
        (System/nanoTime)

        value
        (f)]

    {:value value :ns (- (System/nanoTime) start) :allocated-bytes (- (allocated-bytes) before)}))

(defn- fill-payload!
  [^doubles payload ^long seed]
  (dotimes [lane block-dim]
    (aset payload lane (+ (double seed) lane 0.25)))
  payload)

(defn- build-source
  [{:keys [row-count col-count nnz-per-row] :as config}]
  (let [builder
        (make-overlay-features-builder)

        row-key
        (data/row-key-fn config)

        col-key
        (data/col-key-fn config)

        payload
        (double-array block-dim)]

    (dotimes [row-id row-count]
      (doseq [col-id (data/row-col-ids row-id col-count nnz-per-row)]
        (fill-payload! payload (+ (* row-id col-count) col-id))
        (sparse/append-entry! builder (row-key row-id) (col-key col-id) payload)))
    (csr/dataset->csr-source (overlay-features-freeze! builder))))

(defn- destination
  [^long entry-count]
  {:rows (int-array entry-count)
   :cols (int-array entry-count)
   :values (double-array (* entry-count block-dim))})

(defn- same-output?
  [expected actual]
  (and (Arrays/equals ^ints (:rows expected) ^ints (:rows actual))
       (Arrays/equals ^ints (:cols expected) ^ints (:cols actual))
       (Arrays/equals ^doubles (:values expected) ^doubles (:values actual))))

(defn- copy-visit!
  [source destination cursor row-id col-id origin entry-id block]
  (let [idx
        (aget ^ints cursor 0)

        values
        ^doubles (:values destination)]

    (aset ^ints (:rows destination) idx (int row-id))
    (aset ^ints (:cols destination) idx (int col-id))
    (if (= :main origin)
      (csr/csr-copy-block! source entry-id values (* idx block-dim))
      (System/arraycopy ^doubles block 0 values (* idx block-dim) block-dim))
    (aset ^ints cursor 0 (inc idx))))

(defn- overlay-runner
  [source view selection destination]
  (let [cursor
        (int-array 1)

        visitor
        (fn [row-id _row-key col-id _col-key origin entry-id block]
          (copy-visit! source destination cursor row-id col-id origin entry-id block))]

    (fn []
      (aset cursor 0 0)
      (csr/scan-overlay-selection! view selection visitor)
      (aget cursor 0))))

(defn- compact-overlay
  [source view]
  (let [builder
        (make-overlay-features-builder)

        payload
        (double-array block-dim)]

    (csr/scan-overlay-selection!
      view
      nil
      (fn [_row-id row-key _col-id col-key origin entry-id block]
        (let [value (if (= :main origin) (csr/csr-copy-block! source entry-id payload 0) block)]
          (sparse/append-entry! builder row-key col-key value))))
    (csr/dataset->csr-source (overlay-features-freeze! builder))))

(defn- dirty-row-ids
  [source delta start end]
  (->> (csr/delta-row-keys delta)
       (keep (fn [row-key]
               (let [row-id (csr/csr-row-id source row-key)]
                 (when (and (<= start row-id) (< row-id end)) row-id))))
       sort
       vec))

(defn- split-plan
  [source delta start end]
  (loop [dirty
         (seq (dirty-row-ids source delta start end))

         cursor
         start

         plan
         (transient [])]

    (if-let [row-id (first dirty)]
      (recur (next dirty)
             (inc row-id)
             (cond-> plan
               (< cursor row-id)
               (conj! [:clean cursor row-id])

               true
               (conj! [:dirty row-id])))
      (persistent! (cond-> plan
                     (< cursor end)
                     (conj! [:clean cursor end]))))))

(defn- split-runner
  [source delta plan destination]
  (let [cursor
        (int-array 1)

        visitor
        (fn [row-id _row-key col-id _col-key origin entry-id block]
          (copy-visit! source destination cursor row-id col-id origin entry-id block))]

    (fn []
      (aset cursor 0 0)
      (doseq [[kind start end] plan]
        (if (= :clean kind)
          (let [offset (aget cursor 0)
                copied (csr/csr-copy-ranges! source
                                             [[start end]]
                                             (:rows destination)
                                             (:cols destination)
                                             (:values destination)
                                             offset)]

            (aset cursor 0 (int (+ offset copied))))
          (csr/scan-merged-row-by-id! source delta start visitor)))
      (aget cursor 0))))

(defn- bulk-floor-runner
  [source start end destination]
  #(csr/csr-copy-ranges! source
                         [[start end]]
                         (:rows destination)
                         (:cols destination)
                         (:values destination)
                         0))

(defn- percentile
  ^long [^longs sorted-values percentile]
  (aget sorted-values (max 0 (dec (long (Math/ceil (* percentile (alength sorted-values))))))))

(defn- measure
  [f expected-count reps]
  (assert (= expected-count (long (f))))
  (let [times
        (long-array reps)

        allocations
        (long-array reps)]

    (dotimes [idx reps]
      (let [before (allocated-bytes)
            start (System/nanoTime)
            count (long (f))]

        (assert (= expected-count count))
        (aset times idx (- (System/nanoTime) start))
        (aset allocations idx (- (allocated-bytes) before))))
    (Arrays/sort times)
    (Arrays/sort allocations)
    {:median-ns (aget times (quot reps 2))
     :p95-ns (percentile times 0.95)
     :allocated-bytes (aget allocations (quot reps 2))
     :reps reps}))

(defn- format-bytes
  [bytes]
  (let [units ["B" "KiB" "MiB" "GiB"]]
    (loop [value (double bytes)
           unit 0]

      (if (or (< value 1024.0) (= unit (dec (count units))))
        (format "%.2f %s" value (nth units unit))
        (recur (/ value 1024.0) (inc unit))))))

(defn- selections
  [{:keys [row-count nnz-per-row]}]
  [{:selection :one-row :start 0 :end 1 :entries nnz-per-row}
   {:selection :rows-64
    :start 0
    :end (min 64 row-count)
    :entries (* (min 64 row-count) nnz-per-row)}
   {:selection :full :start 0 :end row-count :entries (* row-count nnz-per-row)}])

(defn- reps-for
  [entry-count smoke?]
  (if smoke? 1 (max 3 (min 20 (quot 2000000 (max 1 (* entry-count block-dim)))))))

(defn- result
  [distribution overlay-count selection backend measurement plan-build]
  (merge {:distribution distribution
          :overlay-count overlay-count
          :selection (:selection selection)
          :backend backend
          :entries (:entries selection)
          :plan-ns (or (:ns plan-build) 0)
          :plan-allocated-bytes (or (:allocated-bytes plan-build) 0)}
         measurement))

(defn- run-selection
  [source delta view compact-source compact-build distribution overlay-count selection destinations
   smoke?]
  (let [{:keys [start end entries]}
        selection

        expected
        (:expected destinations)

        actual
        (:actual destinations)

        current
        (overlay-runner source view {:range {:row-start start :row-end end}} expected)

        plan-build
        (timed #(split-plan source delta start end))

        split
        (split-runner source delta (:value plan-build) actual)

        compacted
        (bulk-floor-runner compact-source start end actual)

        floor
        (bulk-floor-runner source start end actual)

        reps
        (reps-for entries smoke?)]

    (assert (= entries (current)))
    (assert (= entries (split)))
    (assert (same-output? expected actual))
    (assert (= entries (compacted)))
    (assert (same-output? expected actual))
    [(result distribution
             overlay-count
             selection
             :overlay-visitor
             (measure current entries reps)
             nil)
     (result distribution
             overlay-count
             selection
             :split-bulk-merge
             (measure split entries reps)
             plan-build)
     (result distribution
             overlay-count
             selection
             :compacted-bulk
             (measure compacted entries reps)
             compact-build)
     (result distribution
             overlay-count
             selection
             :base-bulk-floor
             (measure floor entries reps)
             nil)]))

(defn- entry-coordinate
  [source {:keys [row-count nnz-per-row]} distribution idx]
  (let [[row-id slot]
        (case distribution
          :clustered
          [(quot idx nnz-per-row) (mod idx nnz-per-row)]

          :scattered
          [(mod idx row-count) (quot idx row-count)])

        [start _]
        (csr/csr-row-span source row-id)

        entry-id
        (+ start slot)

        col-id
        (csr/csr-entry-col-id source entry-id)]

    [(csr/csr-row-key-at source row-id) (csr/csr-col-key-at source col-id)]))

(defn- grow-delta!
  [source delta config distribution from to]
  (let [payload (double-array block-dim)]
    (dotimes [offset (- to from)]
      (let [idx (+ from offset)
            [row-key col-key] (entry-coordinate source config distribution idx)]

        (fill-payload! payload (+ 100000000 idx))
        (csr/delta-put! delta row-key col-key payload))))
  delta)

(defn- print-result
  [{:keys [distribution overlay-count selection backend entries median-ns p95-ns allocated-bytes
           reps plan-ns plan-allocated-bytes]}]
  (println (format "%-9s %,7d %-8s %-17s %,8d %9.3f %9.3f %11s %9.3f %11s %4d"
                   (name distribution)
                   overlay-count
                   (name selection)
                   (name backend)
                   entries
                   (/ median-ns 1000000.0)
                   (/ p95-ns 1000000.0)
                   (format-bytes allocated-bytes)
                   (/ plan-ns 1000000.0)
                   (format-bytes plan-allocated-bytes)
                   reps)))

(defn- checkpoints
  [config smoke?]
  (if smoke? [0 1 10 100] (filterv #(<= % (data/nnz config)) [0 1 10 100 1000 10000 100000])))

(defn- run-distribution
  [source config distribution smoke?]
  (let [delta
        (csr/make-dok-delta-for-source source)

        view
        (csr/overlay-view source delta)

        selection-specs
        (selections config)

        destinations
        (into {}
              (map (fn [{:keys [selection entries]}]
                     [selection {:expected (destination entries) :actual (destination entries)}]))
              selection-specs)]

    (loop [previous
           0

           remaining
           (seq (checkpoints config smoke?))

           results
           []]

      (if-let [overlay-count (first remaining)]
        (let [growth (timed #(grow-delta! source delta config distribution previous overlay-count))]
          (println (format "# grow %-9s %,7d→%,7d: %8.3f ms, %s"
                           (name distribution)
                           previous
                           overlay-count
                           (/ (:ns growth) 1000000.0)
                           (format-bytes (:allocated-bytes growth))))
          (let [compact-build (timed #(compact-overlay source view))
                compact-source (:value compact-build)
                _ (println (format "# compact %,7d: %8.3f ms, %s"
                                   overlay-count
                                   (/ (:ns compact-build) 1000000.0)
                                   (format-bytes (:allocated-bytes compact-build))))
                point-results (vec (mapcat #(run-selection source
                                                           delta
                                                           view
                                                           compact-source
                                                           compact-build
                                                           distribution
                                                           overlay-count
                                                           %
                                                           (get destinations (:selection %))
                                                           smoke?)
                                           selection-specs))]

            (doseq [item point-results]
              (print-result item))
            (recur (long overlay-count) (next remaining) (into results point-results))))
        results))))

(defn run
  [args]
  (let [smoke?
        (some #{"smoke"} args)

        base-config
        (first (data/selected-configs [(if smoke? "small" "medium") "compound"]))

        config
        (if smoke?
          (assoc base-config
            :label "overlay smoke"
            :scale :smoke
            :row-count 512
            :col-count 128
            :nnz-per-row 8)
          base-config)

        source-build
        (timed #(build-source config))

        source
        (:value source-build)]

    (println "sparse-layout overlay growth benchmark")
    (println "Config:"
             (assoc (select-keys config [:key-shape :scale :row-count :col-count :nnz-per-row])
               :nnz (data/nnz config)
               :block-dim block-dim))
    (println "CSR build:" (format "%.1f ms" (/ (:ns source-build) 1000000.0))
             "JVM allocation:" (format-bytes (:allocated-bytes source-build)))
    (println)
    (println (format "%-9s %7s %-8s %-17s %8s %9s %9s %11s %9s %11s %4s" "layout"
                     "overlay" "select"
                     "backend" "entries"
                     "median" "p95"
                     "alloc/call" "prep ms"
                     "prep alloc" "reps"))
    (into {}
          (map (fn [distribution]
                 [distribution (run-distribution source config distribution smoke?)]))
          [:clustered :scattered])))

(defn -main [& args] (run args))
