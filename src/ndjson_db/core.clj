(ns ndjson-db.core
  (:require [clojure.java.io :as jio]
            [cheshire.core :as json]))

(def id-fns (atom {}))

(defn ndjson->idx*
  "Builds up an index of Entity IDs as keys (IDs extracted with id-fn),
  and as value a vector with 2 values:
  the start index in the text file to start read EDN for the JSON doc.,
  and secondly the length in bytes JSON doc."
  [id-fn-key filename] 
  (print "Preparing index for .ndjson database..")
  (with-open [rdr (jio/reader filename)] 
    (loop [lines (line-seq rdr)
           start 0
           index {}]
      (if (seq lines)
        (let [line (first lines)
              len (count (.getBytes ^String line))
              id ((id-fn-key @id-fns) line)]
          (recur (rest lines)
                 (+ start len 1)
                 (assoc index id [start len])))
        (do
          (println " done")
          index)))))

(def ndjson->idx
  (memoize ndjson->idx*))

(defn query-single
  "Queries a single JSON doc. by id in .ndjson file as database,
  returns EDN for the matching doc."
  [id-fn-key filename id]
  (let [[start len] (get (ndjson->idx id-fn-key filename) id)
        bytes (byte-array len)]
    (when (and start len)
      (doto (java.io.RandomAccessFile. filename "r")
        (.seek start)
        (.read bytes 0 len)
        (.close))
      (-> bytes
          (String.)
          (json/parse-string true)))))

(defn query
  "Queries multiple JSON docs by ids in .ndjson file as database,
  returns EDN for the matching JSON doc."
  [{:keys [id-fn-key id-fn filename ids]}]
  {:pre [(keyword? id-fn-key)
         (fn? id-fn)
         (string? filename)
         (sequential? ids)]}
  (swap! id-fns assoc id-fn-key id-fn)
  (keep (partial query-single id-fn-key filename) ids))
