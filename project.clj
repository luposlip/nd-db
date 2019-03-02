(defproject luposlip/ndjson-db "0.1.0"
  :description "Clojure library for using (huge) .ndjson files as lightning fast databases"
  :url "https://github.com/luposlip/ndjson-db"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [cheshire "5.8.1"]]
  :repl-options {:init-ns ndjson-db.core})
