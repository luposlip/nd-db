(ns nd-db.csv
  (:require [clojure
             [string :as s]
             [edn :as edn]]))

(defn csv-row->data
  "Closes over the header, returning a function that takes a row and
   returns a resulting map with keyword keys from header.

   optional param :col-parser is a function that can be used to parse
   individual columns."
  ;; TODO: back to returning af fn, that closes over the initial data!
  [{:keys [cols col-separator col-parser]} row-str]
  {:pre [(string? col-separator)
         (ifn? col-parser)
         (vector? cols)
         (string? row-str)]}
  (zipmap cols
          (->> (s/split row-str (re-pattern col-separator))
               (map col-parser))))
