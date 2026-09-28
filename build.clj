(ns build
  (:require [clojure.string :as str]
            [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as deploy]))

(def lib 'io.github.enajski/sparse-layout)
(def version "0.1.0")
(def class-dir "target/classes")
(def jar-file (str "target/sparse-layout-" version ".jar"))

(defn jar [_]
  (b/delete {:path "target"})
  (let [basis (b/create-basis {:project "deps.edn"})]
    (b/copy-dir {:src-dirs ["src"] :target-dir class-dir})
    (b/write-pom {:class-dir class-dir
                  :lib lib
                  :version version
                  :basis basis
                  :src-dirs ["src"]
                  :scm {:url "https://github.com/enajski/sparse-layout"
                        :connection "scm:git:https://github.com/enajski/sparse-layout.git"}
                  :pom-data [[:description "Sparse layout compiler for Clojure"]
                             [:url "https://github.com/enajski/sparse-layout"]
                             [:licenses
                              [:license
                               [:name "Apache License, Version 2.0"]
                                [:url "https://www.apache.org/licenses/LICENSE-2.0"]]]]})
    (b/jar {:class-dir class-dir :jar-file jar-file}))
  (println "Built" jar-file))

(defn deploy [_]
  (when-not (every? #(not (str/blank? (System/getenv %)))
                    ["CLOJARS_USERNAME" "CLOJARS_PASSWORD"])
    (throw (ex-info "Set CLOJARS_USERNAME and CLOJARS_PASSWORD (deploy token) before publishing"
                    {})))
  (jar nil)
  (deploy/deploy {:installer :remote
                  :artifact jar-file
                  :pom-file (str class-dir "/META-INF/maven/io.github.enajski/sparse-layout/pom.xml")
                  :sign-releases? false}))
