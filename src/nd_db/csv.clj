(ns nd-db.csv
  (:require [clojure.string :as s]))

(defn csv-row->data
  "Closes over the header, returning a function that takes a row and
   returns a resulting map with keyword keys from header.

   optional param :col-parser is a function that can be used to parse
   individual columns."
  [{:keys [cols col-separator col-parser]}]
  {:pre [(string? col-separator)
         (ifn? col-parser)
         (vector? cols)]}
  (let [ptrn (re-pattern col-separator)]
    (fn [row-str]
      {:pre [(string? row-str)]}
      (zipmap cols
              (->> (s/split row-str ptrn)
                   (map col-parser))))))
