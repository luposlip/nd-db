(ns ndjson-db.core
  (:require [clojure.java.io :as jio]
            [cheshire.core :as json]))

(defn ndjson->idx*
  "Builds up an index of Entity IDs as keys,
  and as value a vector with 2 values:
  the start index in the text file to start read EDN for the JSON doc.,
  and secondly the length in bytes JSON doc."
  [filename]
  (print "Preparing index for .ndjson database..")
  (with-open [rdr (jio/reader filename)]
    (let [rx #"^\{\"id\":(\d+)"]
      (loop [lines (line-seq rdr)
             start 0
             index {}]
        (if (seq lines)
          (let [line (first lines)
                [_ idstr] (re-find rx line)
                len (count (.getBytes ^String line))]
            (recur (rest lines)
                   (+ start len 1)
                   (assoc index (Integer. idstr) [start len])))
          (do
            (println " done")
            index))))))

(def ndjson->idx
  (memoize ndjson->idx*))

(defn query-by-id
  "Queries a single JSON doc. by id in .ndjson file as database,
  returns EDN for the matching doc."
  [filename id]
  {:pre [(string? filename)
         (int? id)]}
  (let [[start len] (get (ndjson->idx filename) id)
        bytes (byte-array len)]
    (when (and start len)
      (doto (java.io.RandomAccessFile. filename "r")
        (.seek start)
        (.read bytes 0 len)
        (.close))
      (-> bytes
          (String.)
          (json/parse-string true)))))

(defn ndjson-query
  "Queries multiple JSON docs by ids in .ndjson file as database,
  returns EDN for the matching JSON doc."
  [filename ids] 
  (keep (partial query-by-id filename) ids))
