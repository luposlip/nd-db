(defproject com.luposlip/nd-db "0.9.0-beta11"
  :description "Clojure library to use newline delimited files as fast read-only databases."
  :url "https://github.com/luposlip/nd-db"
  :license {:name "Apache License, Version 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.11.3"]
                 [com.taoensso/nippy "3.4.2"]
                 [org.apache.commons/commons-compress "1.26.2"]
                 [digest "1.4.10"]
                 [cheshire "5.13.0"]]
  :global-vars {*warn-on-reflection* true}
  :repl-options {:init-ns nd-db.core})
