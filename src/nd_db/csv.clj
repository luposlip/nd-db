(ns nd-db.csv
  (:require [clojure
             [string :as s]
             [edn :as edn]]))

(defn csv-row->data
  "Closes over the header, returning a function that takes a row and
   returns a resulting map with keyword keys from header.

   optional param :col-parser is a function that can be used to parse
   individual columns."
  [& {:keys [header separator col-parser drop-n skip-last?]
      :or {drop-n 0
           col-parser identity}}]
  {:pre [(string? header)]}
  (let [cols (->> header
                  (#(s/split % (re-pattern (or separator ","))))
                  (map (comp keyword s/lower-case edn/read-string)))]
    (fn [row]
      {:pre [(string? row)]}
      (zipmap cols
              (->> (s/split row (re-pattern (or separator ",")))
                   (drop drop-n)
                   (#(if skip-last?
                       (butlast %)
                       %))
                   (map col-parser))))))
