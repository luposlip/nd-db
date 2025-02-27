(ns nd-db.query
  (:require [clojure.java.io :as io]
            [nd-db.util :as ndut])
  (:import [java.io File RandomAccessFile]))

(defn- read-nd-doc
  "Takes a doc-parser fn, a nd-db file and start and length.
   Return the document."
  [doc-parser ^File db-file start len extra]
  {:pre [(ifn? doc-parser)]}
  (when (and start len)
    (let [bytes (byte-array len)]
      (doto (RandomAccessFile. db-file "r")
        (.seek start)
        (.read bytes 0 len)
        (.close))
      (if extra
        (doc-parser bytes extra)
        (doc-parser bytes)))))

(defmulti query-docs
  "Queries a single or multiple docs from the database by a single or
   multiple IDs matching those from the `.nd*` database by `id-fn`.
   -  returns EDN for the matching document(s)."
  (fn [_ _ p]
    (cond (sequential? p)
          :sequential
          p
          :single
          :else (throw (ex-info "Unsupported query parameter" {:parameter p})))))

(defmethod query-docs :single
  [index doc-retriever id]
  {:pre [(not (nil? id))]}
  (let [[start len extra] (get index id)]
    (doc-retriever start len extra)))

(defmethod query-docs :sequential
  [index doc-retriever ids]
  {:pre [(sequential? ids)]}
  (keep (partial query-docs index doc-retriever) ids))

(defn doc-retriever-fn
  "Creates a function that can retrieve documents using their position metadata"
  [db]
  (fn [start len extra]
    (read-nd-doc (:doc-parser db) 
                 (io/file ^String (:filename db)) 
                 start len extra)))