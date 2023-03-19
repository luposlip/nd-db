(ns nd-db.convert
  (:require [clojure.string :as s]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [taoensso.nippy :as nippy]
            [nd-db
             [core :as nddb]
             [io :as ndio]
             [util :as util]])
  (:import [java.time Instant]))

(defn db->ndnippy
  "Converts .ndjson and .ndedn files to .ndnippy.

  .ndnippy is MUCH faster (~10x) and requires less memory to process.

  Also the resulting database size _CAN BE_ much smaller:
  ~27GB .ndedn becomes ~12GB .ndnippy (~65% reduction!) in a sample
  with large (>10KB) documents.

  For databases with small documents, the resulting size will be bigger
  than the original.

  Note that this function doesn't create an index, if you want that
  immediately, use ->ndnippy-db instead (which is faster than first
  generating the file and creating the index afterwards)

  But if you only need the index, this function is much faster than
  ->ndnippy-db, since it runs in parallel on available cores."
  [in-db out-filename]
  {:pre [(util/db? in-db)]}
  (with-open [writer (io/writer out-filename)]
    (->> in-db
         :index
         deref
         keys
         (into [])
         (r/map (partial nddb/q in-db))
         (r/map (ndio/append+newline writer))
         (r/fold 50 r/cat r/append!)
         count)))

(defn db->ndnippy-db
  "Converts .ndjson and .ndedn files to .ndnippy, and returns the
  corresponding database.

  Use this function to convert to .ndnippy, _and_ return a database,
  if you need to use the database immediately (or just want to pre-
  create the persistent index for it).

  Since the documents from .ndjson or .ndedn will be parsed anyway,
  you simply supply a path in the document to the ID value via param
  :id-path.

  Alternatively you can use :id-fn for more complex scenarios."
  [in-db {:keys [filename id-path id-fn]}]
  {:pre [(util/db? in-db)
         (string? filename)
         (or (vector? id-path) (fn? id-fn))]
   :post [(util/db? %)]}
  (let [id-fn (or id-fn #(get-in % id-path))
        index (delay (with-meta
                       (into {} (with-open [w (io/writer filename)]
                                  (reduce
                                   (fn [index id]
                                     (let [doc (nddb/q in-db id)
                                           id (id-fn doc)
                                           last-byte-idx (or (some-> index peek (partial apply +)) 0)
                                           byte-size (ndio/append+newline w)]
                                       (conj index [id [(inc last-byte-idx) byte-size]])))
                                   []
                                   (-> @in-db
                                       :index
                                       keys))))
                       {:as-of (Instant/now)}))]
    {:filename filename
     :index index
     :doc-type :nippy
     :as-of (delay (-> @index meta :as-of))}))

(defn paths->idx-id [db-filepath ndmeta-filepath]
  {:pre [(every? string? [db-filepath ndmeta-filepath])]}
  (let [md5 (#'ndio/ndfile-md5 db-filepath)]
    (-> ndmeta-filepath
        (s/split (re-pattern md5))
        last
        (s/split #"\.")
        first)))

(defn upgrade-nddbmeta!
  "Takes nddbmeta filepaths init param.

   If :idx-id is not contained of the old nddbmeta, requires additional
   param for database filepath.

   Upgrades the corresponding nddbmeta file.
   Keeps old file by adding \"_old\" to filename.
   Returns nil if file doesn't exist.
   Logs if already upgraded."
  [{:keys [filename serialized-filename]}]
  (when (.isFile (io/file serialized-filename))
    (try
      (let [{:keys [version idx-id timestamp] :as db-info}
            (nippy/thaw-from-file serialized-filename)
            idx-id (or idx-id (paths->idx-id filename serialized-filename))]
        (when-not version
          (ndio/mv-file serialized-filename (str serialized-filename "_old"))
          (ndio/serialize-db (-> (dissoc db-info :timestamp)
                                 (assoc :version "0.9.0" ;; TODO version!
                                        :idx-id idx-id
                                        :as-of (delay timestamp)))
                             (:index db-info)
                             serialized-filename)
          :upgraded))
      (catch Exception _
        :already-upgraded))))
