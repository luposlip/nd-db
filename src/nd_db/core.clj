(ns nd-db.core
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [cheshire.core :as json]
            [nd-db
             [io :as ndio]
             [index :as ndix]
             [util :as ndut]])
  (:import [java.io File RandomAccessFile BufferedReader]))

(defn parse-doc [db doc-str]
  (case (:doc-type @db)
    :json (json/parse-string doc-str true)
    :edn (edn/read-string doc-str)
    :nippy (ndio/str-> doc-str)
    :else (throw (ex-info "Unknown doc-type" {:doc-type @db}))))

(defn- raw-db
  "Creates a database var which can be used to perform queries"
  [& {:keys [id-fn filename] :as params}]
  {:pre [(-> params meta :parsed?)]}
  (future (let [index (ndix/create-index filename id-fn)]
            (-> params
                (dissoc id-fn)
                (assoc :as-of (-> index meta :as-of str)
                       :version "0.9.0"
                       :index index)))))

(defn- persisted-db [params]
  (let [serialized-filepath (ndio/serialized-db-filepath params)]
    (if (.isFile ^File (io/file serialized-filepath))
      (ndio/parse-db params serialized-filepath)
      (let [[_ serialized-filename] (ndio/path->folder+filename serialized-filepath)]
        (ndio/serialize-db
         (raw-db
          (assoc params
                 :serialized-filename serialized-filename)))))))

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
      (persisted-db params)
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

(defn read-nd-doc
  "Takes a doc-parser fn, a nd-db file and start and length.
   Return the document."
  [doc-parser ^File db-file start len]
  (when (and start len)
    (let [bytes (byte-array len)]
      (doto (RandomAccessFile. db-file "r")
        (.seek start)
        (.read bytes 0 len)
        (.close))
      (doc-parser (String. bytes)))))

(defmethod q :single query-single
  [db id]
  {:pre [(ndut/db? db)
         (not (nil? id))]}
  (let [[start len] (get (:index @db) id)
        nd-file (io/file ^String (:filename @db))]
    (read-nd-doc (partial parse-doc db) nd-file start len)))

(defmethod q :sequential query-multiple
  [db ids]
  {:pre [(sequential? ids)]}
  (keep (partial q db) ids))

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
   (lazy-docs-eager-idx db (lazy-seq (:index @db))))
  ([a b]
   {:pre [(every? (some-fn future? (partial instance? BufferedReader)) [a b])]}
   (let [[db] (filter future? [a b])
         [reader] (filter (partial instance? BufferedReader) [a b])]
     (lazy-docs-lazy-idx ndio/str->
                         (io/file (:filename @db))
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
    (->> @db :index (map first))

    (= BufferedReader (class i))
    (lazy-ids-lazy-idx ndio/str-> i)

    :else (throw (ex-info "Pass either db or index-reader!" {:param-type (type i)}))))
