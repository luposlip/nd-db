(defproject com.luposlip/nd-db "0.5.2"
  :description "Clojure library that treats newline delimited files as simple databases."
  :url "https://github.com/luposlip/nd-db"
  :license {:name "Apache License, Version 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [com.taoensso/nippy "3.1.1"]
                 [buddy/buddy-core "1.10.1"]
                 [digest "1.4.10"]
                 [cheshire "5.10.1"]]
  :repl-options {:init-ns nd-db.core})
