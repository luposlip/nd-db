(ns nd-db.core
  (:require [clojure
             [string :as str]]
            [clojure.java.io :as io]
            [clarch.core :as clarch]
            [nd-db
             [io :as ndio]
             [index :as ndix]
             [zip-index :as ndzx]
             [util :as ndut]]
            [nd-db.core :as nddb])
  (:import [java.io File RandomAccessFile BufferedReader]))

(defn- raw-db
  "Creates a database var which can be used to perform queries"
  [& {:keys [id-fn filename doc-type] :as params}]
  {:pre [(-> params meta :parsed?)]}
  (let [index (delay (ndix/create-index
                      filename id-fn
                      (when (= :csv doc-type)
                        :skip-first!)))]
    (-> params
        (dissoc :id-fn)
        (assoc :version "0.9.0" ;; TODO version!
               :index index
               :as-of (delay (-> @index meta :as-of))))))

(defn- persisted-db [params]
  (let [serialized-filepath (ndio/serialized-db-filepath params)]
    (if (.isFile ^File (io/file serialized-filepath))
      (ndio/parse-db params serialized-filepath)
      (let [[_ serialized-filename] (ndio/path->folder+filename serialized-filepath)]
        (->> (assoc params :serialized-filename serialized-filename)
             raw-db
             ndio/serialize-db
             (ndix/re-index (:log-limit params)))))))

(defn zip-db [& {:as p}]
  {:post [(ndut/db? %)]}
  (let [parsed (ndio/parse-params p)
        serialized-filepath (ndio/serialized-db-filepath parsed)]
    (update
     (if (.isFile ^File (io/file serialized-filepath))
       (ndio/parse-db parsed serialized-filepath)
       (let [[_ serialized-filename] (ndio/path->folder+filename serialized-filepath)
             index (delay (ndzx/zip-index parsed))]
         (-> parsed
             (assoc :serialized-filename serialized-filename
                    :version "0.9.0"
                    :index index
                    :as-of (delay (-> @index meta :as-of)))
             ndio/serialize-db
             ;;(ndix/re-index (:log-limit params)) ;; ever needed for zip?
             )))
     :doc-parser
     (fn [doctype-parser]
       (comp doctype-parser clarch/zip-bytes->uncompressed-bytes)))))

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
  :id-path        - Use with nippy databases. Docs can be indexed directly by path vector
  :id-type        - The type of data to store as ID (key) in the index
  :source-type    - If the source-type is different from the ID type to store in the index
  :index-folder   - Folder to persist index in, defaults to system temp folder
  :index-persist? - Set to false to inhibit storing the index on disk, defaults to true. Will also
                    inhibit the use of previously persisted indices!
  :filename       - .ndnippy input filename (full path)
  :log-limit      - Read the documents log until and including this index (for versioning)"
  [& _params]
  {:post [(ndut/db? %)]}
  (let [{:keys [index-persist?] :as params} (apply ndio/parse-params _params)]
    (if index-persist?
      (persisted-db params)
      (raw-db params))))

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

(defmethod q :single query-single
  [db id]
  {:pre [(ndut/db? db)
         (not (nil? id))]}
  (let [[start len extra] (get @(:index db) id)
        nd-file (io/file ^String (:filename db))]
    (read-nd-doc (:doc-parser db) nd-file start len extra)))

(defmethod q :sequential query-multiple
  [db ids]
  {:pre [(sequential? ids)]}
  (keep (partial q db) ids))

(defn or-q [dbs id-or-ids]
  {:pre [(every? ndut/db? dbs)]}
  (when (and (coll? id-or-ids)
             (not (map? id-or-ids)))
    (throw (ex-info "or-q currently only support single IDs!" {:id-or-ids id-or-ids})))
  (loop [dbs dbs]
    (when (seq dbs)
      (or (q (first dbs) id-or-ids)
          (recur (rest dbs))))))

(defn- lazy-docs-eager-idx
  "This will work for any database metadata version.
   If no metadata exist, that will be generated first (which may take a while
   for big databases).

   Furthermore huge databases will have their index realized, before returning
   the lazy seq of documents.

   If this is not wanted, get a with-open a nd-db.io/db->reader+parser and call
   lazy-docs with that."
  [db ids]
  (lazy-seq
   (when (seq ids)
     (cons (q db (ffirst ids))
           (lazy-docs-eager-idx db (rest ids))))))

(defn- lazy-docs-lazy-idx [nippy-parser nd-file ^BufferedReader reader]
  (lazy-seq
   (when-let [line (.readLine reader)]
     (cons (let [[_ [start len]] (nippy-parser line)]
             (read-nd-doc nippy-parser nd-file start len))
           (lazy-docs-lazy-idx nippy-parser nd-file reader)))))

(defn lazy-docs
  ([db]
   {:pre [(ndut/db? db)]}
   (when (:version db)
     (println "You should use (with-open [r (nd-db.index/reader db)] (lazy-docs db r)) instead!")
     (lazy-docs-eager-idx db (lazy-seq @(:index db)))))
  ([a b]
   {:pre [(every? (some-fn map? (partial instance? BufferedReader)) [a b])]}
   (let [both [a b]
         [db] (filter map? both)
         [reader] (filter (partial instance? BufferedReader) both)]
     (lazy-docs-lazy-idx ndio/str->
                         (io/file (:filename db))
                         reader))))

(defn- lazy-ids-lazy-idx [nippy-parser ^BufferedReader reader]
  (lazy-seq
   (when-let [line (.readLine reader)]
     (cons (-> line nippy-parser first)
           (lazy-ids-lazy-idx nippy-parser reader)))))

(defn lazy-ids
  "Returns a lazy seq of the IDs in the index.
   When using index-reader, the order is guaranteed to be the same as the
   order in the database."
  [i]
  (when (and (ndut/db? i)
             (ndut/v090+? i))
    (println "For true laziness pass nd-db.index/reader to lazy-ids!"))

  (cond
    (ndut/db? i)
    (->> i :index deref (map first))

    (= BufferedReader (class i))
    (lazy-ids-lazy-idx ndio/str-> i)

    :else (throw (ex-info "Pass either db or index-reader!" {:param-type (type i)}))))

(defn- emit-docs [db ^String doc-emission-str]
  "Emit a serialized document(s) to a database.
  NOT thread safe, only use this from a single thread,
  meaning DON'T do parallel writes to database..!"
  {:pre [(ndut/db? db)
         (string? doc-emission-str)]}
  (with-open [w (ndio/append-writer (:filename db))]
    (.write w doc-emission-str)
    (.newLine w)
    (.flush w))
  db)

(defn append [{:keys [doc-emitter log-limit] :as db} doc-or-docs]
  {:pre [((some-fn map? sequential?) doc-or-docs)]}
  (when (or (nil? doc-emitter)
            log-limit)
    (throw
     (ex-info
      "Can't write when doc-emitter is nil or to a historical database (when log-limit is set)!" {:doc-emitter doc-emitter
                                                                                                  :log-limit log-limit})))
  (let [all-docs (if (map? doc-or-docs)
                   [doc-or-docs]
                   doc-or-docs)]
    (loop [docs-parts (partition-all 128 all-docs)
           aggr-db db]
      (if (empty? docs-parts)
        aggr-db
        (let [docs-part (first docs-parts)
              docs-part-stringed (map doc-emitter docs-part)]
          (recur (rest docs-parts)
                 (-> aggr-db
                     (emit-docs (->> docs-part-stringed (str/join "\n")))
                     (ndix/append docs-part docs-part-stringed))))))))
