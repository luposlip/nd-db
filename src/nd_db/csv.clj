(ns nd-db.csv
  (:require [clojure
             [edn :as edn]
             [string :as s]]
            [nd-db.util :as ndut]))

(defn default-col-parser
  [col-str]
  (if (ndut/number-str? col-str)
    (edn/read-string col-str)
    col-str))

(defn col-str->key-vec
  "Converts a CSV key column string to a vector of keywords.
   Trims and lower-cases before converting to keyword."
  [col-split-pattern col-str]
  (mapv (comp keyword s/lower-case s/trim) (s/split col-str col-split-pattern)))

(defn csv-row->data
  "Closes over the header, returning a function that takes a row and
   returns a resulting map with keyword keys from header.

   optional param :col-parser is a function that is used to parse
   individual columns - defaults to default-col-parser."
  [& {:keys [cols col-separator col-parser] :or {col-parser default-col-parser}}]
  {:pre [(string? col-separator)
         (ifn? col-parser)
         ((some-fn vector? string?) cols)]}
  (let [ptrn (re-pattern col-separator)
        cols (if (vector? cols)
               cols
               (col-str->key-vec ptrn cols))]
    (fn [row-str]
      {:pre [(string? row-str)]}
      (zipmap cols
              (->> (s/split row-str ptrn)
                   (map col-parser))))))
