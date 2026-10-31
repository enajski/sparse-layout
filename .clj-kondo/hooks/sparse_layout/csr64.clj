(ns hooks.sparse-layout.csr64
  (:require [clj-kondo.hooks-api :as api]))

(defn- vector-of?
  [node counts]
  (and (api/vector-node? node) (contains? counts (count (:children node)))))

(defn reduce-entries
  "Analyzes `(reduce-entries [row col e] [src row-start row-end] [acc init] & body)` as
  `(do src row-start row-end (let [acc init row 0 col 0 e 0] body))`."
  [{:keys [node]}]
  (let [[_ entry-node range-node acc-node & body] (:children node)]
    (when (and (vector-of? entry-node #{3})
               (vector-of? range-node #{1 3})
               (vector-of? acc-node #{2}))
      (let [[row col e] (:children entry-node)
            [acc init] (:children acc-node)
            zero (api/token-node 0)
            bindings (api/vector-node [acc init row zero col zero e zero])
            body-node (api/list-node (list* (api/token-node 'let) bindings body))]

        {:node (api/list-node
                 (concat [(api/token-node 'do)] (:children range-node) [body-node]))}))))
