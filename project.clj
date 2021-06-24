(defproject com.luposlip/nd-db "0.4.0"
  :description "Clojure library that treats lines in newline delimited files as simple databases."
  :url "https://github.com/luposlip/nd-db"
  :license {:name "Apache License, Version 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [cheshire "5.10.0"]]
  :repl-options {:init-ns nd-db.core})
