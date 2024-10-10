(defproject com.luposlip/nd-db "0.9.0-beta15"
  :description "Clojure library to use newline delimited files as fast read-only databases."
  :url "https://github.com/luposlip/nd-db"
  :license {:name "Apache License, Version 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [com.taoensso/nippy "3.4.2"]
                 [com.luposlip/clarch "0.3.3"]
                 [digest "1.4.10"]
                 [com.cnuernber/charred "1.034"]]
  :global-vars {*warn-on-reflection* true}
  :repl-options {:init-ns nd-db.core})
