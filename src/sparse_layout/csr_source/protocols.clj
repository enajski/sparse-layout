(ns sparse-layout.csr-source.protocols)

(defprotocol CSRSource
  "Storage-level fixed-double-block CSR access.

  Row ids are valid in [0, csr-row-count), column ids in [0, csr-col-count),
  and entry ids in [0, csr-entry-count). Implementations should reject ids
  outside those ranges rather than reading arbitrary storage."
  (csr-row-count [src])
  (csr-col-count [src])
  (csr-entry-count [src])
  (csr-block-dim [src])
  (csr-row-id [src row-key])
  (csr-col-id [src col-key])
  (csr-row-key-at [src row-id])
  (csr-col-key-at [src col-id])
  (csr-row-span [src row-id])
  (csr-entry-col-id [src entry-id])
  (csr-copy-block! [src entry-id dst dst-off]
    "Copies one fixed-width payload block into dst at dst-off and returns dst.")
  (csr-copy-ranges! [src ranges dst-row-ids dst-col-ids dst-values dst-entry-off]
    "Copies ranges in CSR scan order into caller-owned primitive arrays.
    `dst-entry-off` indexes the row/column arrays and the corresponding fixed
    block in `dst-values`. Returns the number of copied entries.")
  (csr-scan-row! [src row-id visitor])
  (csr-resolve-ranges [src selection])
  (csr-scan-ranges! [src ranges visitor]))

(defprotocol MutableSparseDelta
  "Mutable row/column overlay for CSRSource scans.

  Delta puts must use the same fixed block dimension as the source they will be
  merged with."
  (delta-put! [delta row-key col-key block])
  (delta-delete! [delta row-key col-key])
  (delta-row-keys [delta])
  (delta-row-entries [delta row-key]
    "Returns a cached deterministic row. Generic deltas use printed-key order;
    deltas made for a source use that source's CSR column order.")
  (delta-entry [delta row-key col-key])
  (delta-clear! [delta])
  (delta-version [delta]))

(defprotocol MergedCSRSource
  (scan-merged-row! [main delta row-key visitor])
  (scan-merged-ranges! [main delta ranges visitor]
    "Scans base CSR row-id ranges with delta entries overlaid. This does not
    discover delta-only rows because numeric ranges address only base rows."))
