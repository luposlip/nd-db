(ns nd-db.csv
  (:require [clojure
             [edn :as edn]
             [string :as str]]
            [nd-db.util :as ndut]))

(defn default-col-parser
  [col-str]
  (if (ndut/number-str? col-str)
    (edn/read-string col-str)
    col-str))

(def default-col-emitter str)

(defn col-str->key-vec
  "Converts a CSV key column string to a vector of keywords.
   Trims and lower-cases before converting to keyword."
  [col-split-pattern col-str]
  (mapv (comp keyword str/lower-case str/trim) (str/split col-str col-split-pattern)))

(defn- ensure-cols [ptrn cols]
  (if (vector? cols)
    cols
    (col-str->key-vec ptrn cols)))

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
        cols (ensure-cols ptrn cols)]
    (fn [row-str]
      {:pre [(string? row-str)]}
      (zipmap cols
              (->> (str/split row-str ptrn)
                   (map col-parser))))))

(defn data->csv-row
  "Closes over the header, returning a function that transforms data to a CSV
   row with data in same order as the header.

   optional param :col-emitter is a function that is used to emit
   individual columns - defaults to default-col-emitter."
  [& {:keys [cols col-separator col-emitter] :or {col-emitter default-col-emitter}}]
  {:pre [(string? col-separator)
         (ifn? col-emitter)
         ((some-fn vector? string?) cols)]}
  (let [cols (ensure-cols (re-pattern col-separator) cols)]
    (fn [doc]
      {:pre [(map? doc)]}
      (->> cols
           (reduce
            (fn [a i]
              (conj a (col-emitter (i doc))))
            [])
           (str/join ",")))))
