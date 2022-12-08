(defproject com.luposlip/nd-db "0.7.2"
  :description "Clojure library that lets you use newline delimited files as databases. Fast!"
  :url "https://github.com/luposlip/nd-db"
  :license {:name "Apache License, Version 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [com.taoensso/nippy "3.2.0"]
                 [buddy/buddy-core "1.10.413"]
                 [digest "1.4.10"]
                 [cheshire "5.11.0"]]
  :global-vars {*warn-on-reflection* true}
  :repl-options {:init-ns nd-db.core})
