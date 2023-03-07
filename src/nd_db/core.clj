(ns nd-db.core
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [cheshire.core :as json]
            [nd-db
             [util :as ndut]
             [io :as ndio]
             [index :as ndix]])
  (:import [java.io File RandomAccessFile FileReader BufferedReader]))

(defn parse-doc [db doc-str]
  (case (:doc-type @db)
    :json (json/parse-string doc-str true)
    :edn (edn/read-string doc-str)
    :nippy (ndio/str-> doc-str)
    :else (throw (ex-info "Unknown doc-type" {:doc-type @db}))))

(defn- raw-db
  "Creates a database var which can be used to perform queries"
  [{:keys [id-fn filename doc-type idx-id] :as params}]
  {:pre [(-> params meta :parsed?)]}
  (future (let [index (ndix/create-index filename id-fn)]
            {:filename filename
             :index index
             :doc-type doc-type
             :timestamp (-> index meta :timestamp str)
             :idx-id idx-id
             :version "0.9.0+"})))

(defn- persisted-db [params]
  (let [serialized-filename (ndio/serialize-db-filename params)]
    (if (.isFile ^File (io/file serialized-filename))
      (ndio/parse-db params serialized-filename)
      (ndio/serialize-db serialized-filename (raw-db params)))))

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
  {:pre [(sequential? ids)]}
  (keep (partial q db) ids))

(defn warn-legacy []
  (println
   "WARN: "))

(defn- lazy-docs-db
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
     (let [doc (q db (ffirst ids))]
       (cons doc
             (lazy-docs-db db (rest ids)))))))

(defn- lazy-docs-index-reader [doc-parser ^BufferedReader reader]
  (when-let [line (.readLine reader)]
    (cons (doc-parser line) (lazy-docs-index-reader doc-parser reader))))

(defmulti lazy-docs
  (fn [p]
    (cond (ndut/db? p) :db
          (instance? BufferedReader p) :index-reader
          :else (throw (ex-info "Unable to create lazy seq of docs with input"
                                {:param p
                                 :type (type p)})))))

(defmethod lazy-docs :db [db]
  (lazy-docs-db db (lazy-seq (:index @db))))

(defmethod lazy-docs :index-reader [index-reader]
  (lazy-docs-index-reader ndio/str-> index-reader))

(defn index-reader
  "Returns a BufferedReader of the database index.
   Use this in a with-open block (or close it explicitly when done)!"
  ^BufferedReader [db]
  {:post [(instance? BufferedReader %)]}
  (when-not (and (:idx-id @db)
                 (= :nippy (:doc-type @db))
                 (:version @db))
    (throw (ex-info "ERROR: pre v0.9.0 .nddbmeta format - cannot lazily traverse index.
Consider converting the index (or delete it, which will auto-recreate it)."
                    (dissoc @db :index))))
  (let [sfn (ndio/serialize-db-filename @db)
        r (BufferedReader.
           (FileReader.
            sfn))]
    (.readLine r)
    r))
