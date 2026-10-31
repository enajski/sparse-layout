(ns sparse-layout.csr-source-notebook
  {:nextjournal.clerk/no-cache true}
  (:require [clojure.string :as str]
            [nextjournal.clerk :as clerk]
            [sparse-layout.core :refer [block-view-value]]
            [sparse-layout.csr-source :as csr])
  (:import [java.nio.file Paths]))

;; # sparse-layout: from logical records to query-shaped CSR
;;
;; This notebook is executable documentation for the storage path in
;; `sparse-layout`. It walks from a nested logical shape to a frozen CSR layout,
;; then layers a query-shaped range index and mutable delta over that immutable
;; main store.
;;
;; The central idea is simple: keep the public data model pleasant, but compile
;; the physical layout into primitive arrays so hot readers can scan rows and
;; copy fixed-width blocks without allocating a new block per entry.

^{::clerk/visibility {:code :hide}}
(clerk/html [:div {:style {:padding "1.1rem 1.25rem" :border-radius "8px"}}
             [:h3 {:style {:margin "0 0 .35rem"}} "What this notebook demonstrates"]
             [:ol {:style {:margin "0" :padding-left "1.25rem"}}
              [:li "Compile nested Clojure records into primitive-array CSR."]
              [:li "Draw the logical sparse grid directly from the data."]
              [:li "Adapt the frozen dataset to the lower-level CSRSource protocol."]
              [:li "Render the physical CSR anatomy from the live arrays."]
              [:li "Resolve query prefixes into row ranges."]
              [:li "Overlay a mutable DOK delta on top of immutable CSR."]
              [:li "Focus overlay scans on a row, a row set, a prefix, or a range."]
              [:li "Visualize the merged overlay, including delta-only rows."]]])

;; ## 1. Declare a sparse physical shape
;;
;; The demo dataset is a network of weather stations. Each row is one station
;; keyed by `[region country city]`, each column is one sensor metric, and each
;; present coordinate stores a full day of hourly samples. `defsparse` describes
;; that shape and the target payload representation:
;;
;; ```clojure
;; {:row-key [:row]
;;  :cols-path [:metrics]
;;  :payload {:kind :fixed-double-block
;;            :dim 24}
;;  :indices #{:csr :csc}}
;; ```
;;
;; Each coordinate stores a 24-wide double block: one sample per hour, midnight
;; to 23:00. Not every station carries every sensor — that missing coverage is
;; the sparsity the layout compiles away.

;; The synthetic series are deterministic diurnal curves: a base level plus a
;; cosine swing peaking at a chosen hour.

(defn diurnal-series
  "24 hourly samples: `base` plus a cosine-shaped swing of `amplitude` peaking
  at `peak-hour`. Values are rounded to one decimal."
  [base amplitude peak-hour]
  (mapv (fn [hour]
          (let [angle (* 2.0 Math/PI (/ (- hour peak-hour) 24.0))]
            (/ (Math/round (* 10.0 (+ base (* amplitude (Math/cos angle))))) 10.0)))
        (range 24)))

;; The rows below are intentionally ordered by `[region country city]`.
;; That order is what later makes prefix ranges cheap to resolve.

(def ^:nextjournal.clerk/no-cache demo-state
  (do
    (eval '(sparse-layout.core/defsparse
            station-day-metrics
            {:cols-path [:metrics]
             :indices #{:csc :csr}
             :payload {:dim 24 :kind :fixed-double-block}
             :row-key [:row]}))
    (let [records
          [{:row [:amer :us :birmingham]
            :metrics (array-map :pressure (diurnal-series 1015.0 3.5 4)
                                :humidity (diurnal-series 68.0 -14.0 15))}
           {:row [:apac :jp :tokyo] :metrics (array-map :temperature (diurnal-series 21.0 6.0 14))}
           {:row [:emea :de :berlin]
            :metrics (array-map :temperature (diurnal-series 14.0 7.0 15)
                                :pressure (diurnal-series 1009.0 4.0 2)
                                :humidity (diurnal-series 72.0 -18.0 16))}
           {:row [:emea :pl :krakow]
            :metrics (array-map :temperature (diurnal-series 11.5 8.0 14)
                                :humidity (diurnal-series 78.0 -20.0 15))}
           {:row [:emea :uk :birmingham]
            :metrics (array-map :temperature (diurnal-series 9.5 4.5 13)
                                :pressure (diurnal-series 1002.0 5.0 20))}]

          compile!
          @(requiring-resolve 'sparse-layout.csr-source-notebook/station-day-metrics-compile)

          row-value
          @(requiring-resolve 'sparse-layout.csr-source-notebook/station-day-metrics-row)

          block-view
          @(requiring-resolve 'sparse-layout.csr-source-notebook/station-day-metrics-block-view)

          dataset
          (compile! records)]

      {:records records
       :dataset dataset
       :sample-row (row-value dataset [:emea :de :berlin])
       :temperature-view (block-view dataset [:emea :de :berlin] :temperature)
       :source (csr/dataset->csr-source dataset)})))

(def records (:records demo-state))

(def dataset (:dataset demo-state))

(clerk/table records)

;; ## 2. What the data looks like
;;
;; A table hides the defining property of this dataset: most coordinates are
;; absent. The grid below is generated directly from `records` — every present
;; cell sparklines its 24-hour series scaled to the column's min–max range, and
;; every dotted cell is a coordinate the layout never has to store.

^{::clerk/visibility {:code :hide}}
(def metric-units {:temperature "°C" :pressure "hPa" :humidity "%"})

^{::clerk/visibility {:code :hide}}
(defn row-key-label [row-key] (str/join " · " (map name row-key)))

^{::clerk/visibility {:code :hide}} (defn fmt-num [v] (format "%.1f" (double v)))

^{::clerk/visibility {:code :hide}}
(defn series-stats
  [block]
  {:min (apply min block) :max (apply max block) :mean (/ (reduce + 0.0 block) (count block))})

^{::clerk/visibility {:code :hide}}
(defn column-ranges
  "Per-column `[min max]` over every present block, used to scale sparklines."
  [cells]
  (reduce (fn [acc [[_row-key col-key] {:keys [block]}]]
            (if block
              (update acc
                      col-key
                      (fn [[lo hi]]
                        [(min (or lo ##Inf) (apply min block))
                         (max (or hi ##-Inf) (apply max block))]))
              acc))
          {}
          cells))

^{::clerk/visibility {:code :hide}}
(defn sparkline-points
  "SVG polyline points for `block`, scaled into a box at `x`,`y` of size
  `w`×`h` using the value range `[lo hi]`."
  [block [lo hi] x y w h]
  (let [lo
        (double lo)

        span
        (max (- (double hi) lo) 1.0E-9)

        n
        (count block)]

    (str/join " "
              (map-indexed (fn [i v]
                             (let [px
                                   (+ x (* i (/ (double w) (dec n))))

                                   py
                                   (+ y (- h (* (/ (- (double v) lo) span) h)))]

                               (str (format "%.1f" px) "," (format "%.1f" py))))
                           block))))

^{::clerk/visibility {:code :hide}}
(def origin-styles
  {:main {:color "#6366f1" :border "1px solid #e2e8f0" :background "#ffffff" :label "main CSR"}
   :delta {:color "#d97706" :border "1px solid #fcd34d" :background "#fffbeb" :label "delta put"}
   :deleted {:color "#ef4444"
             :border "1px dashed #fca5a5"
             :background "#fef2f2"
             :label "delta delete (suppressed)"}})

^{::clerk/visibility {:code :hide}}
(defn legend-swatch
  [color label & [dashed?]]
  [:span {:style {:display "inline-flex" :align-items "center" :gap ".35rem" :margin-right "1rem"}}
   [:span
    {:style {:width ".7rem"
             :height ".7rem"
             :border-radius "3px"
             :background (if dashed? "transparent" color)
             :border (when dashed? (str "1px dashed " color))
             :display "inline-block"}}]
   [:span {:style {:font-size ".72rem" :color "#475569"}} label]])

^{::clerk/visibility {:code :hide}}
(defn grid-cell
  [ranges col-key {:keys [origin block]}]
  (if (nil? origin)
    [:div
     {:style {:border "1px dashed #e2e8f0"
              :border-radius "8px"
              :min-height "84px"
              :display "flex"
              :align-items "center"
              :justify-content "center"
              :color "#cbd5e1"
              :font-size "1.1rem"}} "·"]
    (let [{:keys [color border background]}
          (origin-styles origin)

          deleted?
          (= :deleted origin)

          stats
          (series-stats block)

          unit
          (get metric-units col-key "")]

      [:div
       {:style {:border border
                :background background
                :border-radius "8px"
                :padding ".45rem .55rem"
                :min-height "84px"}}
       [:svg {:viewBox "0 0 96 26" :style {:width "100%" :height "26px" :display "block"}}
        [:polyline
         {:points (sparkline-points block (get ranges col-key) 2 3 92 20)
          :fill "none"
          :stroke (if deleted? "#cbd5e1" color)
          :stroke-width 1.5
          :stroke-linejoin "round"
          :stroke-linecap "round"}]]
       [:div
        {:style {:font-weight 600
                 :font-size ".85rem"
                 :margin-top ".2rem"
                 :text-decoration (when deleted? "line-through")
                 :color (if deleted? "#94a3b8" (if (= :delta origin) "#b45309" "#1e293b"))}}
        (str (fmt-num (:mean stats)) " " unit)]
       [:div {:style {:font-size ".65rem" :color "#94a3b8"}}
        (str "min " (fmt-num (:min stats)) " · max " (fmt-num (:max stats)))]
       (when (not= :main origin)
         [:span
          {:style {:font-size ".6rem"
                   :font-weight 700
                   :color color
                   :text-transform "uppercase"
                   :letter-spacing ".04em"}} (if deleted? "deleted" "Δ put")])])))

^{::clerk/visibility {:code :hide}}
(defn render-sparse-grid
  "Renders a logical sparse selection as an HTML grid. `cells` maps
  `[row-key col-key]` to `{:origin ... :block [...]}`. Optional `row-meta` maps
  row keys to `{:sub ... :badge ...}`. Optional `focus` is
  `{:row-keys #{...} :caption ...}` — rows outside the focus set are dimmed."
  [{:keys [title subtitle col-keys row-keys cells row-meta focus]}]
  (let [ranges
        (column-ranges cells)

        focus-row-keys
        (:row-keys focus)

        origins
        (set (keep :origin (vals cells)))]

    [:div
     {:style {:background "#f8fafc"
              :border "1px solid #e2e8f0"
              :border-radius "10px"
              :padding "1rem 1.1rem"
              :overflow-x "auto"
              :font-family "system-ui, sans-serif"}}
     [:div {:style {:font-weight 700 :color "#0f172a" :font-size ".95rem"}} title]
     (when subtitle
       [:div {:style {:color "#64748b" :font-size ".78rem" :margin-top ".15rem"}} subtitle])
     (when-let [caption (:caption focus)]
       [:div {:style {:color "#6366f1" :font-size ".75rem" :font-weight 600 :margin-top ".3rem"}}
        caption])
     [:div {:style {:margin-top ".55rem"}}
      (concat (map (fn [origin]
                     (let [{:keys [color label]} (origin-styles origin)]
                       (legend-swatch color label)))
                   (sort origins))
              [(legend-swatch "#cbd5e1" "absent (never stored)" true)])]
     (into
       [:div
        {:style {:display "grid"
                 :grid-template-columns
                 (str "180px repeat(" (count col-keys) ", minmax(120px, 1fr))")
                 :gap ".4rem"
                 :margin-top ".8rem"
                 :align-items "stretch"}} [:div]]
       (concat
         (map (fn [col-key]
                [:div
                 {:style {:font-size ".72rem"
                          :font-weight 700
                          :color "#475569"
                          :text-transform "uppercase"
                          :letter-spacing ".05em"
                          :align-self "end"
                          :padding-bottom ".2rem"}}
                 (str (name col-key)
                      (when-let [unit (get metric-units col-key)]
                        (str " (" unit ")")))])
              col-keys)
         (mapcat (fn [row-key]
                   (let [dimmed?
                         (and focus-row-keys (not (contains? focus-row-keys row-key)))

                         {:keys [sub badge]}
                         (get row-meta row-key)

                         row-style
                         {:opacity (if dimmed? 0.25 1) :filter (when dimmed? "grayscale(0.8)")}]

                     (cons [:div
                            {:style (merge row-style
                                           {:display "flex"
                                            :flex-direction "column"
                                            :justify-content "center"})}
                            [:div {:style {:font-weight 600 :font-size ".8rem" :color "#1e293b"}}
                             (row-key-label row-key)]
                            (when sub [:div {:style {:font-size ".68rem" :color "#94a3b8"}} sub])
                            (when badge
                              [:div
                               {:style {:font-size ".6rem"
                                        :font-weight 700
                                        :color "#0d9488"
                                        :text-transform "uppercase"
                                        :letter-spacing ".04em"}} badge])]
                           (map (fn [col-key]
                                  [:div {:style row-style}
                                   (grid-cell ranges col-key (get cells [row-key col-key]))])
                                col-keys))))
                 row-keys)))]))

^{::clerk/visibility {:code :hide}}
(def record-col-keys (vec (distinct (mapcat (comp keys :metrics) records))))

^{::clerk/visibility {:code :hide}}
(def record-cells
  (into {}
        (for [{:keys [row metrics]}
              records

              [col-key block]
              metrics]

          [[row col-key] {:origin :main :block block}])))

^{::clerk/visibility {:code :hide}}
(clerk/html
  (render-sparse-grid
    {:title (str "Logical sparse shape — "
                 (count records)
                 " stations × "
                 (count record-col-keys)
                 " metric columns, "
                 (count record-cells)
                 " of "
                 (* (count records) (count record-col-keys))
                 " cells present")
     :subtitle
     "Generated from the raw records. Each cell sparklines its 24-hour series, scaled to the column's min–max range."
     :col-keys record-col-keys
     :row-keys (mapv :row records)
     :cells record-cells}))

;; ## 3. The generated API stays ergonomic
;;
;; Existing callers can keep using the generated row, column, point, and view
;; APIs. The new source layer is additive.

(def sample-row (:sample-row demo-state))

sample-row

;; Zero-copy block views read single hours without materializing the series —
;; here Berlin's temperature at midnight, midday, and 23:00.

(def temperature-view (:temperature-view demo-state))

[(block-view-value temperature-view 0) (block-view-value temperature-view 12)
 (block-view-value temperature-view 23)]

;; ## 4. The frozen shape is compact CSR plus dictionaries
;;
;; The source adapter exposes the important CSR facts without exposing generated
;; dataset classes to hot consumers.

(def source (:source demo-state))

(def physical-summary
  {:rows (csr/csr-row-count source)
   :entries (csr/csr-entry-count source)
   :payload-kind :fixed-double-block
   :block-dim (csr/csr-block-dim source)
   :payload-doubles (* (csr/csr-entry-count source) (csr/csr-block-dim source))})

(clerk/table [physical-summary])

;; ## 5. Adapt the frozen dataset to CSRSource
;;
;; `CSRSource` is the storage boundary. Consumers ask for spans, column ids, and
;; block copies; they do not need to know whether the backing store is a heap
;; array today or an mmap artifact later.

(defn block-values
  [^doubles blocks block-dim offset]
  (mapv (fn [j]
          (aget blocks (+ offset j)))
        (range block-dim)))

(defn copied-block-values
  [source entry-id]
  (let [out (double-array (csr/csr-block-dim source))]
    (csr/csr-copy-block! source entry-id out 0)
    (vec (seq out))))

(defn materialized-scan->table
  [{:keys [source rows row-keys cols col-keys entries origins blocks count]}]
  (let [block-dim (csr/csr-block-dim source)]
    (mapv (fn [i]
            (let [row-id (aget ^ints rows i)
                  col-id (aget ^ints cols i)]

              (cond-> {:row-id row-id
                       :row-key (if row-keys
                                  (aget ^objects row-keys i)
                                  (when-not (neg? row-id) (csr/csr-row-key-at source row-id)))
                       :entry-id (aget ^ints entries i)
                       :col-id col-id
                       :col-key
                       (if col-keys (aget ^objects col-keys i) (csr/csr-col-key-at source col-id))
                       :block (block-values blocks block-dim (* i block-dim))}
                origins
                (assoc :origin (aget ^objects origins i)))))
          (range count))))

(defn materialize-source-row
  [source row-id]
  (let [[start end]
        (csr/csr-row-span source row-id)

        entry-count
        (- end start)

        block-dim
        (csr/csr-block-dim source)

        rows
        (int-array entry-count)

        cols
        (int-array entry-count)

        entries
        (int-array entry-count)

        blocks
        (double-array (* entry-count block-dim))

        cursor
        (int-array 1)]

    (csr/csr-scan-row! source
                       row-id
                       (fn [row-id* col-id entry-id src]
                         (let [i
                               (aget cursor 0)

                               block-off
                               (* i block-dim)]

                           (aset-int rows i row-id*)
                           (aset-int cols i col-id)
                           (aset-int entries i entry-id)
                           (csr/csr-copy-block! src entry-id blocks block-off)
                           (aset-int cursor 0 (inc i)))))
    {:source source :rows rows :cols cols :entries entries :blocks blocks :count (aget cursor 0)}))

(def row-catalog
  (mapv (fn [row-id]
          (let [[start end] (csr/csr-row-span source row-id)]
            {:row-id row-id
             :row-key (csr/csr-row-key-at source row-id)
             :entry-start start
             :entry-end end
             :entries (- end start)}))
        (range (csr/csr-row-count source))))

(clerk/table row-catalog)

;; ### The physical anatomy, drawn from the live arrays
;;
;; Everything below is read through the `CSRSource` protocol: the pointer array
;; via `csr-row-span`, entry columns via `csr-entry-col-id`, and payload doubles
;; via `csr-copy-block!`. Each pointer marks where its row's entries begin in
;; the flat entry strip; the final sentinel pointer marks the end. Each entry's
;; 24 payload doubles are drawn as a sparkline scaled to its own min–max.

^{::clerk/visibility {:code :hide}}
(def row-palette ["#6366f1" "#0ea5e9" "#10b981" "#f59e0b" "#ec4899" "#8b5cf6"])

^{::clerk/visibility {:code :hide}}
(defn csr-anatomy-svg
  "Draws row pointers, the entry strip, and payload blocks of any CSRSource."
  [source]
  (let [row-count
        (csr/csr-row-count source)

        entry-count
        (csr/csr-entry-count source)

        block-dim
        (csr/csr-block-dim source)

        spans
        (mapv #(csr/csr-row-span source %) (range row-count))

        ptr-values
        (conj (mapv first spans) entry-count)

        ptr-x
        (fn [i]
          (+ 16 (* i 64)))

        entry-x
        (fn [e]
          (+ 16 (* e 100)))

        row-color
        (fn [r]
          (nth row-palette (mod r (count row-palette))))

        width
        (max (+ (ptr-x (count ptr-values)) 24) (+ (entry-x entry-count) 24))

        mono
        "ui-monospace, SFMono-Regular, Menlo, monospace"]

    (into
      [:svg
       {:viewBox (str "0 0 " width " 316")
        :xmlns "http://www.w3.org/2000/svg"
        :style {:max-width "100%"
                :height "auto"
                :background "#ffffff"
                :border "1px solid #e2e8f0"
                :border-radius "10px"}}
       [:text
        {:x 16
         :y 18
         :font-size 11
         :font-weight 700
         :fill "#475569"
         :font-family "system-ui, sans-serif"}
        "csr-row-ptrs (int[]) — one entry offset per row, plus the end sentinel"]]
      (concat
        ;; pointer boxes with values and captions
        (mapcat (fn [i ptr]
                  (let [x
                        (ptr-x i)

                        last?
                        (= i row-count)]

                    [[:rect
                      {:x x
                       :y 28
                       :width 56
                       :height 30
                       :rx 6
                       :fill (if last? "#f1f5f9" "#eef2ff")
                       :stroke (if last? "#cbd5e1" "#c7d2fe")}]
                     [:text
                      {:x (+ x 28)
                       :y 48
                       :font-size 13
                       :font-weight 600
                       :text-anchor "middle"
                       :fill "#1e293b"
                       :font-family mono} (str ptr)]
                     [:text
                      {:x (+ x 28)
                       :y 72
                       :font-size 9
                       :text-anchor "middle"
                       :fill "#94a3b8"
                       :font-family mono} (if last? "end" (str "row " i))]]))
                (range)
                ptr-values)
        ;; connectors from each pointer to the start of its row's entry span
        (map (fn [r]
               (let [[start _end]
                     (nth spans r)

                     x1
                     (+ (ptr-x r) 28)

                     x2
                     (+ (entry-x start) 6)]

                 [:path
                  {:d (str "M " x1 " 58 C " x1 " 92, " x2 " 82, " x2 " 112")
                   :fill "none"
                   :stroke (row-color r)
                   :stroke-width 1.4
                   :opacity 0.8}]))
             (range row-count))
        [[:path
          {:d (let [x1
                    (+ (ptr-x row-count) 28)

                    x2
                    (- (entry-x entry-count) 8)]

                (str "M " x1 " 58 C " x1 " 92, " x2 " 82, " x2 " 112"))
           :fill "none"
           :stroke "#94a3b8"
           :stroke-width 1.2
           :stroke-dasharray "4 3"}]]
        ;; per-row bands over the entry strip
        (mapcat (fn [r]
                  (let [[start end]
                        (nth spans r)

                        color
                        (row-color r)

                        x
                        (- (entry-x start) 4)]

                    [[:rect
                      {:x x
                       :y 112
                       :width (- (* 100 (- end start)) 2)
                       :height 170
                       :rx 8
                       :fill color
                       :fill-opacity 0.07
                       :stroke color
                       :stroke-opacity 0.4}]
                     [:text
                      {:x (+ x 8)
                       :y 127
                       :font-size 10
                       :font-weight 700
                       :fill color
                       :font-family "system-ui, sans-serif"}
                      (row-key-label (csr/csr-row-key-at source r))]]))
                (range row-count))
        ;; entry boxes: id, payload offset, column, payload sparkline
        (mapcat
          (fn [e]
            (let [x
                  (entry-x e)

                  cx
                  (+ x 46)

                  col-id
                  (csr/csr-entry-col-id source e)

                  col-key
                  (csr/csr-col-key-at source col-id)

                  block
                  (copied-block-values source e)

                  stats
                  (series-stats block)

                  owner
                  (some (fn [r]
                          (let [[start end] (nth spans r)]
                            (when (and (<= start e) (< e end)) r)))
                        (range row-count))]

              [[:rect {:x x :y 134 :width 92 :height 140 :rx 6 :fill "#ffffff" :stroke "#e2e8f0"}]
               [:text
                {:x cx :y 150 :font-size 8 :text-anchor "middle" :fill "#94a3b8" :font-family mono}
                (str "entry " e " · @" (* e block-dim))]
               [:text
                {:x cx :y 166 :font-size 10 :text-anchor "middle" :fill "#64748b" :font-family mono}
                (str "col-id "
                     col-id
                     (when-let [unit (get metric-units col-key)]
                       (str " · " unit)))]
               [:text
                {:x cx
                 :y 184
                 :font-size 11
                 :font-weight 700
                 :text-anchor "middle"
                 :fill "#334155"
                 :font-family mono} (str col-key)]
               [:polyline
                {:points (sparkline-points block [(:min stats) (:max stats)] (+ x 10) 196 72 28)
                 :fill "none"
                 :stroke (row-color owner)
                 :stroke-width 1.5
                 :stroke-linejoin "round"
                 :stroke-linecap "round"}]
               [:text
                {:x cx :y 244 :font-size 9 :text-anchor "middle" :fill "#475569" :font-family mono}
                (str "min " (fmt-num (:min stats)))]
               [:text
                {:x cx :y 258 :font-size 9 :text-anchor "middle" :fill "#475569" :font-family mono}
                (str "max " (fmt-num (:max stats)))]]))
          (range entry-count))
        [[:text {:x 16 :y 304 :font-size 10 :fill "#94a3b8" :font-family "system-ui, sans-serif"}
          (str "payload: one flat double["
               (* entry-count block-dim)
               "] — "
               entry-count
               " entries × "
               block-dim
               " doubles per block (hourly samples, midnight → 23:00)")]]))))

^{::clerk/visibility {:code :hide}} (clerk/html (csr-anatomy-svg source))

;; Scan one row through the storage-level API. The visitor writes ids into
;; preallocated `int[]` arrays and copies payloads into one caller-owned
;; contiguous `double[]`.

(def row-0-scan (materialize-source-row source 0))

(clerk/table (materialized-scan->table row-0-scan))

;; ## 6. Query-shaped row ranges
;;
;; A range index maps query prefixes to row ranges. With stations ordered by
;; `[region country city]`, a region or region+country query can resolve to a
;; small set of contiguous row spans.

(def ranged-source (csr/with-range-index source identity))

(def emea-ranges (csr/csr-resolve-ranges ranged-source {:prefix [:emea]}))

emea-ranges

(defn range-entry-count
  [source ranges]
  (reduce (fn [n row-range]
            (let [[row-start row-end] ((fn [r]
                                         (cond (map? r) [(:row-start r) (:row-end r)]
                                               :else r))
                                        row-range)]
              (+ n
                 (reduce (fn [row-n row-id]
                           (let [[start end] (csr/csr-row-span source row-id)]
                             (+ row-n (- end start))))
                         0
                         (range row-start row-end)))))
          0
          ranges))

(defn materialize-ranges
  [source ranges]
  (let [entry-count
        (range-entry-count source ranges)

        block-dim
        (csr/csr-block-dim source)

        rows
        (int-array entry-count)

        cols
        (int-array entry-count)

        entries
        (int-array entry-count)

        blocks
        (double-array (* entry-count block-dim))

        cursor
        (int-array 1)]

    (csr/csr-scan-ranges! source
                          ranges
                          (fn [row-id col-id entry-id src]
                            (let [i
                                  (aget cursor 0)

                                  block-off
                                  (* i block-dim)]

                              (aset-int rows i row-id)
                              (aset-int cols i col-id)
                              (aset-int entries i entry-id)
                              (csr/csr-copy-block! src entry-id blocks block-off)
                              (aset-int cursor 0 (inc i)))))
    {:source source :rows rows :cols cols :entries entries :blocks blocks :count (aget cursor 0)}))

(clerk/table (materialized-scan->table (materialize-ranges ranged-source emea-ranges)))

;; Non-contiguous projections are still correct. This projection indexes only
;; the city segment, and Birmingham exists in both `:amer :us` and `:emea :uk`,
;; so it returns two ranges.

(def by-city-source
  (csr/with-range-index source
                        (fn [[_region _country city]]
                          [city])))

(csr/csr-resolve-ranges by-city-source {:prefix [:birmingham]})

;; ## 7. Mutable delta over immutable CSR
;;
;; The DOK delta records puts and deletes without rewriting the main CSR — here
;; a recalibrated temperature series for Berlin, its decommissioned barometer, a
;; humidity sensor newly installed in Birmingham UK, and a brand-new Stockholm
;; station that has not been compacted into the main store yet. During overlay
;; scans:
;;
;; 1. A delta put overrides the main coordinate.
;; 2. A delta delete suppresses the main coordinate.
;; 3. Delta-only rows with visible puts are emitted by logical selections.
;; 4. Output remains deterministic by column id.

(def delta
  (doto (csr/make-dok-delta-for-source source)
    (csr/delta-put! [:emea :de :berlin] :temperature (diurnal-series 15.2 6.5 15))
    (csr/delta-delete! [:emea :de :berlin] :pressure)
    (csr/delta-put! [:emea :uk :birmingham] :humidity (diurnal-series 81.0 -16.0 14))
    (csr/delta-put! [:emea :se :stockholm] :temperature (diurnal-series 8.0 6.0 14))))

(defn merged-row-capacity
  [source delta row-key]
  (+ (let [row-id (csr/csr-row-id source row-key)]
       (if (neg? row-id)
         0
         (let [[start end] (csr/csr-row-span source row-id)]
           (- end start))))
     (count (csr/delta-row-entries delta row-key))))

(defn materialize-merged-row
  [source delta row-key]
  (let [entry-capacity
        (merged-row-capacity source delta row-key)

        block-dim
        (csr/csr-block-dim source)

        rows
        (int-array entry-capacity)

        cols
        (int-array entry-capacity)

        entries
        (int-array entry-capacity)

        row-keys
        (object-array entry-capacity)

        col-keys
        (object-array entry-capacity)

        origins
        (object-array entry-capacity)

        blocks
        (double-array (* entry-capacity block-dim))

        cursor
        (int-array 1)]

    (csr/scan-merged-row! source
                          delta
                          row-key
                          (fn [row-id row-key* col-id col-key origin entry-id block]
                            (let [i
                                  (aget cursor 0)

                                  block-off
                                  (* i block-dim)]

                              (aset-int rows i row-id)
                              (aset-int cols i col-id)
                              (aset-int entries i entry-id)
                              (aset row-keys i row-key*)
                              (aset col-keys i col-key)
                              (aset origins i origin)
                              (if (= :main origin)
                                (csr/csr-copy-block! source entry-id blocks block-off)
                                (System/arraycopy ^doubles block 0 blocks block-off block-dim))
                              (aset-int cursor 0 (inc i)))))
    {:source source
     :rows rows
     :row-keys row-keys
     :cols cols
     :col-keys col-keys
     :entries entries
     :origins origins
     :blocks blocks
     :count (aget cursor 0)}))

(clerk/table (materialized-scan->table (materialize-merged-row source delta [:emea :de :berlin])))

;; Delta-only rows are supported too. They use row id `-1` because they are not
;; yet present in the immutable main CSR.

(clerk/table (materialized-scan->table
               (materialize-merged-row source delta [:emea :se :stockholm])))

;; ## 8. Overlay views and focused scans
;;
;; `overlay-view` wraps a source and a delta into one logical view, and
;; `scan-overlay-selection!` is how consumers focus that view on exactly the
;; slice they care about. Selections come in five shapes:
;;
;; | Selection | Focus | Delta-only rows |
;; |---|---|---|
;; | `nil` | everything | included |
;; | `{:row-key k}` | one logical row | included when visible |
;; | `{:row-keys [...]}` | explicit row set, caller order, deduplicated | included when visible |
;; | `{:prefix [...]}` | range-index rows for the prefix | matching ones appended |
;; | `{:range ...}` / `{:ranges ...}` | numeric base row ids | excluded (no stable row id) |

(def overlay (csr/overlay-view ranged-source delta))

(defn collect-overlay-selection
  [source overlay selection]
  (let [seen (atom [])]
    (csr/scan-overlay-selection!
      overlay
      selection
      (fn [row-id row-key col-id col-key origin entry-id block]
        (swap! seen conj
          {:row-id row-id
           :row-key row-key
           :col-id col-id
           :col-key col-key
           :origin origin
           :entry-id entry-id
           :block (if (= :main origin) (copied-block-values source entry-id) (vec (seq block)))})))
    @seen))

;; **Focus on one logical row.** Berlin's delta put overrides `:temperature`,
;; the delete suppresses `:pressure`, and `:humidity` still reads from the
;; main CSR.

(clerk/table (collect-overlay-selection ranged-source overlay {:row-key [:emea :de :berlin]}))

;; **Focus on an explicit row set.** Caller order is preserved and duplicates
;; are dropped, so the delta-only Stockholm station can lead the output, and
;; Birmingham UK's freshly installed humidity sensor is merged in column order.

(clerk/table (collect-overlay-selection ranged-source
                                        overlay
                                        {:row-keys [[:emea :se :stockholm] [:emea :uk :birmingham]
                                                    [:emea :se :stockholm]]}))

;; **Focus on a prefix.** The range index resolves base rows, then matching
;; visible delta-only rows are appended — Sweden has no base rows at all, so
;; only the new Stockholm station is emitted.

(clerk/table (collect-overlay-selection ranged-source overlay {:prefix [:emea :se]}))

;; **Focus on numeric ranges.** These stay base-row-id scans: delta puts and
;; deletes still apply to base rows, but delta-only rows are excluded because
;; they have no stable CSR row id yet.

(clerk/table (collect-overlay-selection ranged-source overlay {:range {:row-start 0 :row-end 2}}))

;; ## 9. The overlay, drawn from the data
;;
;; The grid below is a full overlay scan (`nil` selection) with the delta's
;; delete decorations added back in, so all four overlay effects are visible at
;; once: an overridden series, a suppressed sensor, a brand-new sensor in an
;; existing station, and an entirely delta-only station.

(defn base-block
  [source row-key col-key]
  (let [row-id
        (csr/csr-row-id source row-key)

        found
        (volatile! nil)]

    (when-not (neg? row-id)
      (csr/csr-scan-row! source
                         row-id
                         (fn [_row-id col-id entry-id src]
                           (when (= col-key (csr/csr-col-key-at src col-id))
                             (vreset! found (copied-block-values src entry-id))))))
    @found))

(def overlay-cells
  (merge (into {}
               (map (fn [{:keys [row-key col-key origin block]}]
                      [[row-key col-key] {:origin origin :block block}]))
               (collect-overlay-selection ranged-source overlay nil))
         (into {}
               (for [row-key
                     (csr/delta-row-keys delta)

                     {:keys [op col-key]}
                     (csr/delta-row-entries delta row-key)

                     :when (= :delete op)
                     :let [block
                           (base-block source row-key col-key)]
                     :when block]

                 [[row-key col-key] {:origin :deleted :block block}]))))

(def base-row-keys (mapv #(csr/csr-row-key-at source %) (range (csr/csr-row-count source))))

(def delta-only-row-keys
  (->> (csr/delta-row-keys delta)
       (filter #(neg? (csr/csr-row-id source %)))
       (sort-by pr-str)
       vec))

(def overlay-row-keys (into base-row-keys delta-only-row-keys))

(def overlay-col-keys
  (vec (distinct (concat (map #(csr/csr-col-key-at source %) (range (csr/csr-col-count source)))
                         (map second (keys overlay-cells))))))

(def overlay-row-meta
  (into {}
        (map (fn [row-key]
               (let [row-id (csr/csr-row-id source row-key)]
                 [row-key
                  (if (neg? row-id)
                    {:sub "row-id −1" :badge "delta-only"}
                    {:sub (str "row " row-id)})])))
        overlay-row-keys))

^{::clerk/visibility {:code :hide}}
(clerk/html
  (render-sparse-grid
    {:title "Merged overlay — immutable CSR with the DOK delta scanned over it"
     :subtitle
     "Full overlay scan (nil selection). Amber cells are delta puts (a recalibrated series and a new sensor), the struck-through cell is a decommissioned sensor suppressed by a delta delete, and the delta-only station has no CSR row id yet."
     :col-keys overlay-col-keys
     :row-keys overlay-row-keys
     :cells overlay-cells
     :row-meta overlay-row-meta}))

;; And the same grid focused through a prefix selection: rows outside the
;; resolved ranges are dimmed, exactly mirroring what
;; `scan-overlay-selection!` would emit for `{:prefix [:emea]}` — including the
;; delta-only Stockholm station, which matches the prefix.

(def focus-prefix [:emea])

(def focused-row-keys
  (set (map :row-key (collect-overlay-selection ranged-source overlay {:prefix focus-prefix}))))

^{::clerk/visibility {:code :hide}}
(clerk/html
  (render-sparse-grid
    {:title (str "Focused overlay — {:prefix " (pr-str focus-prefix) "}")
     :subtitle "Dimmed rows are outside the selection and are never visited by the scan."
     :focus {:row-keys focused-row-keys
             :caption
             (str "resolves to base ranges "
                  (pr-str (csr/csr-resolve-ranges ranged-source {:prefix focus-prefix}))
                  " → "
                  (count focused-row-keys)
                  " logical rows, "
                  (count (collect-overlay-selection ranged-source overlay {:prefix focus-prefix}))
                  " merged entries")}
     :col-keys overlay-col-keys
     :row-keys overlay-row-keys
     :cells overlay-cells
     :row-meta overlay-row-meta}))

;; ## 10. Persist and reopen as a language-neutral mmap artifact
;;
;; The artifact writer accepts any `CSRSource`, not just generated datasets. The
;; file is a single little-endian binary artifact with a header, section table,
;; primitive CSR sections, payload doubles, and typed row/column key
;; dictionaries.

(def artifact-path (Paths/get "/tmp/sparse-layout-demo.slcsr" (make-array String 0)))

(csr/write-csr-artifact! source artifact-path)

(def mmap-source (csr/open-csr-artifact artifact-path))

(clerk/table [(select-keys (csr/csr-artifact-metadata mmap-source)
                           [:row-count :col-count :entry-count :block-dim])])

(clerk/table (materialized-scan->table (materialize-source-row mmap-source 0)))

;; The anatomy renderer only speaks `CSRSource`, so pointing it at the reopened
;; artifact reproduces the exact same physical picture — now read through a
;; mapped byte buffer instead of heap arrays.

^{::clerk/visibility {:code :hide}} (clerk/html (csr-anatomy-svg mmap-source))

;; ## 11. What mmap needs to preserve
;;
;; Other languages do not need Clojure internals to read the core layout. They
;; need only the binary header, the section table, primitive sections, and the
;; tagged key dictionary encoding.

(def mmap-artifact-format
  {:header [:magic :version :endian-marker :header-length :section-count :section-table-offset
            :row-count :col-count :entry-count :block-dim :flags]
   :section-table [:section-id :primitive-type :count :byte-offset :byte-length]
   :sections [:row-ptrs :col-ids :payload-values :row-key-offsets :row-key-bytes :col-key-offsets
              :col-key-bytes :metadata]
   :key-tags [:nil :false :true :int64 :float64 :string :keyword :vector]})

(clerk/table (mapv (fn [[field value]]
                     {:format-field field :value value})
                   mmap-artifact-format))

;; ## 12. Takeaway
;;
;; `sparse-layout` is moving toward a physical layout compiler:
;;
;; - `defsparse` keeps the logical API concise.
;; - Freeze compiles nested data into dictionary-encoded primitive arrays.
;; - `CSRSource` decouples hot readers from generated dataset types.
;; - Range indexes encode query shape without changing the public dataset API.
;; - Mmap artifacts make the physical layout durable and language-neutral.
;; - Overlay views make mutable deltas visible without pretending they are
;;   stable CSR sources, and selections focus scans on rows, row sets,
;;   prefixes, or numeric ranges.

(def delta-op-counts
  (frequencies (map :op (mapcat #(csr/delta-row-entries delta %) (csr/delta-row-keys delta)))))

^{::clerk/visibility {:code :hide}}
(clerk/html
  [:div {:style {:display "grid" :grid-template-columns "repeat(5, minmax(0, 1fr))" :gap ".75rem"}}
   (for [[label value]
         [["Stations" (csr/csr-row-count source)] ["Entries" (csr/csr-entry-count source)]
          ["Block dim" (csr/csr-block-dim source)] ["Delta puts" (get delta-op-counts :put 0)]
          ["Delta deletes" (get delta-op-counts :delete 0)]]]
     [:div
      {:style
       {:padding "1rem" :border-radius "8px" :background "#f8fafc" :border "1px solid #e2e8f0"}}
      [:div {:style {:font-size ".8rem" :text-transform "uppercase" :color "#64748b"}} label]
      [:div {:style {:font-size "1.8rem" :font-weight "700" :color "#0f172a"}} value]])])
