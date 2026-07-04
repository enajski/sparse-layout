(ns hooks.sparse-layout.core
  (:require [clj-kondo.hooks-api :as api]
            [clojure.string :as str]))

(defn- pascal-case
  [value]
  (->> (str/split (name value) #"-")
       (remove str/blank?)
       (map str/capitalize)
       (apply str)))

(defn- token [value] (api/token-node value))

(defn- def-node [sym init-node] (api/list-node [(token 'def) (token sym) init-node]))

(defn- defn-node
  [sym]
  (api/list-node [(token 'defn) (token sym) (api/vector-node [(token '&) (token '_args)])
                  (api/list-node [(token 'do)])]))

(defn- protocol-node
  [protocol-sym method-sym]
  (api/list-node [(token 'defprotocol) (token protocol-sym)
                  (api/list-node [(token method-sym) (api/vector-node [(token 'this)])])]))

(defn- deftype-node
  [type-sym]
  (api/list-node [(token 'deftype) (token type-sym) (api/vector-node [])]))

(defn defsparse
  [{:keys [node]}]
  (let [[_ layout-name-node layout-node]
        (:children node)

        layout-name
        (api/sexpr layout-name-node)

        base-name
        (name layout-name)

        type-name
        (symbol (pascal-case layout-name))

        marker-protocol-name
        (symbol (str "Sparse" (pascal-case layout-name)))

        generated-fns
        (mapv #(symbol (str base-name %))
              ["-compile" "-row-id" "-col-id" "-row" "-col" "-block" "-row-views" "-col-views"
               "-block-view" "-freeze!" "-ingest!"])

        prefixed-fns
        [(symbol (str "make-" base-name "-builder"))]

        nodes
        (concat [(def-node (symbol (str base-name "-layout")) layout-node)
                 (protocol-node marker-protocol-name (symbol (str base-name "-dataset?")))
                 (deftype-node type-name)]
                (map defn-node (concat prefixed-fns generated-fns)))]

    {:node (api/list-node (into [(token 'do)] nodes))}))
