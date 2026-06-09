(ns sparse-layout.memory
  (:gen-class)
  (:require [clj-memory-meter.core :as mm]
            [sparse-layout.bench-data :as data]))

(defn- format-bytes [bytes]
  (let [units ["B" "KiB" "MiB" "GiB"]
        bytes (double bytes)]
    (loop [value bytes
           unit 0]
      (if (or (< value 1024.0)
              (= unit (dec (count units))))
        (format "%.2f %s" value (nth units unit))
        (recur (/ value 1024.0) (inc unit))))))

(defn- measure-bytes [value]
  (long (mm/measure value :bytes true)))

(defn- print-memory-row [config label bytes]
  (let [entries (data/nnz config)]
    (println (format "%-52s %14s %12.1f B/nnz"
                     label
                     (format-bytes bytes)
                     (/ (double bytes) entries)))))

(defn- memory-config [config]
  (data/print-config config)
  (let [{:keys [entries sparse nested-rows nested-dual nested-vectorz]} (data/prepare config)]
    (println)
    (println "Retained object graph size, measured independently from each root:")
    (print-memory-row config "flat source entries" (measure-bytes entries))
    (print-memory-row config "sparse frozen dataset, CSR+CSC" (measure-bytes sparse))
    (print-memory-row config "nested row-only index (persistent vector)" (measure-bytes nested-rows))
    (print-memory-row config "nested row-only index (vectorz)" (measure-bytes nested-vectorz))
    (print-memory-row config "nested dual row+column index" (measure-bytes nested-dual))))

(defn memory-report
  ([] (memory-report (data/standard-configs)))
  ([configs]
   (println "sparse-layout memory report")
   (println "Using clj-memory-meter retained-size measurement.")
   (doseq [config configs]
     (memory-config config))))

(defn -main [& args]
  (memory-report (data/selected-configs args)))
