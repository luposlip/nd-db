(ns nd-db.core
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [cheshire.core :as json]
            [nd-db
             [util :as ndut]
             [io :as ndio]])
  (:import [java.time Instant]
           [java.io File RandomAccessFile]))

(defn reducr [id-fn]
  (fn [acc line]
    (let [len (count (.getBytes ^String line))
          id (id-fn line)
          [_ start plen] (or (peek acc) [nil -1 0])]
      ;; TODO concat into list for parallelization
      (conj acc [id (+ 1 start plen) len]))))

(defn combinr
  ([] []) ;; TODO: Lazify for parallelization
  ([_] [])
  ([acc more]
   (let [[_ prev-start prev-len] (or (peek acc) [nil -1 0])
         prev-offset (+ 1 prev-start prev-len)]
     (reduce
      (fn [a [id old-start len]]
        (conj a [id (+ prev-offset old-start) len]))
      acc
      more))))

(defn create-index
  "Builds up an index of Entity IDs as keys (IDs extracted with id-fn),
  and as value a vector with 2 values:
  - the start index in the text file to start read EDN for the input doc
  - the length in bytes input doc"
  [filename id-fn]
  {:pre [(string? filename)
         (fn? id-fn)]}
  (with-open [rdr (io/reader filename)]
    (->> rdr
         line-seq ;; for parallel processing, enable line below!
         ;;(into [])
         (r/fold (or (some-> (System/getenv "NDDB_LINES_PER_CORE")
                             edn/read-string)
                     512)
                 combinr
                 (reducr id-fn))
         (reduce
          (fn [acc i]
            (assoc acc (first i) (into [] (rest i))))
          {}))))

(defn index-id
  "This function generates a pseudo unique index ID for the combination
  of the ID function and the filename."
  [& {:keys [filename id-fn]}]
  (with-open [in (io/reader filename)]
    (mapv id-fn (take 10 (line-seq in)))))

(defn raw-db
  "Creates a database var which can be used to perform queries"
  [{:keys [id-fn filename doc-type] :as params}]
  {:pre [(-> params meta :parsed?)]}
  (future {:filename filename
           :index (create-index filename id-fn)
           :doc-type doc-type
           :timestamp (str (Instant/now))}))

(defn db
  "Tries to read the specified pre-parsed database from filesystem.
  If this cannot be found, creates and persists a new one.

  Params:
  :id-rx-str      - Regular expression STRING REPRESENTATION to retrieve unique ID from the data.
                    The second match is used as the ID. The string is used to name the serialized index.
                    NB: Regular expressions is often much faster than parsing and inspecting data!
  :id-fn          - For more complex parsing/indexing, you can supply a function instead of
                    a regular expression. Only do this, if you are certain it's faster than regex!
                    NB: Only the content of the database will be used to name the persisted index.
                    This using different ID functions will overwrite previously persisted indices
                    where :id-fn is also used!
  :id-name        - Convenience parameter - if you just want to supply the name of the ID in the text
                    based data to search for - creates a regex under the hood. Should be used with
                    the next parameter for optimal speed.
  :id-type        - The type of data to store as ID (key) in the index
  :source-type    - If the source-type is different from the ID type to store in the index
  :index-folder   - Folder to persist index in, defaults to system temp folder
  :index-persist? - Set to false to inhibit storing the index on disk, defaults to true. Will also
                    inhibit the use of previously persisted indices!
  :filename       - .ndnippy input filename (full path)
  :index-path     - Use with .ndnippy file, docs can be index directly by path vector"
  [& _params]
  {:post [(ndut/db? %)]}
  (let [{:keys [index-persist?] :as params} (apply ndio/parse-params _params)]
    (if index-persist?
      (let [serialized-filename (ndio/serialize-db-filename params)]
        (if (.isFile ^File (io/file serialized-filename))
          (ndio/parse-db params serialized-filename)
          (ndio/serialize-db serialized-filename (raw-db params))))
      (raw-db params))))

(defmulti q
  "Queries a single or multiple docs from the database by a single or
  multiple IDs matching those from the `.nd*` database by `id-fn`.
  -  returns EDN for the matching document."
  (fn [_ p]
    (cond (sequential? p)
          :sequential
          p
          :single
          :else (throw (ex-info "Unsupported query parameter" {:parameter p})))))

(defn parse-doc [db doc-str]
  (condp = (:doc-type @db)
    :json (json/parse-string doc-str true)
    :edn (edn/read-string doc-str)
    :nippy (ndio/str-> doc-str)
    (throw (ex-info "Unknown doc-type" {:doc-type (:doc-type @db)}))))

(defmethod q :single query-single
  [db id]
  {:pre [(ndut/db? db)
         (not (nil? id))]}
  (let [[start len] (get (:index @db) id)
        bytes (byte-array len)]
    (when (and start len)
      (doto (RandomAccessFile. ^String (:filename @db) "r")
        (.seek start)
        (.read bytes 0 len)
        (.close))
      (->> bytes
           (String.)
           (parse-doc db)))))

(defmethod q :sequential query-multiple
  [db ids]
  {:pre [(ndut/db? db)
         (sequential? ids)]}
  (keep (partial q db) ids))
