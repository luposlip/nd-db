(defproject luposlip/ndjson-db "0.2.2"
  :description "Clojure library for using (huge) .ndjson files as lightning fast databases"
  :url "https://github.com/luposlip/ndjson-db"
  :license {:name "Apache License, Version 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [com.taoensso/timbre "5.1.0"]
                 [org.clojure/core.memoize "1.0.236"]
                 [cheshire "5.10.0"]]
  :repl-options {:init-ns ndjson-db.core})
