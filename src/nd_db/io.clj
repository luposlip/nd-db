(ns nd-db.io
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as s]
            [taoensso.nippy :as nippy]
            digest
            [nd-db
             [util :as ndut]
             [id-fns :as ndid]])
  (:import [java.io File BufferedInputStream Writer BufferedWriter]
           [org.apache.commons.compress.compressors CompressorInputStream CompressorStreamFactory]))

(defn tmpdir []
  (System/getProperty "java.io.tmpdir"))

(defn ndfile-md5
  "Reads first 10 lines of file, return corresponding MD5"
  [filename]
  (with-open [r (io/reader filename)]
    (let [input (take 10 (line-seq r))]
      (digest/md5 (s/join input)))))

(defn ->str ^String [data]
  (nippy/freeze-to-string data))

(defn str-> [^String data-str]
  (nippy/thaw-from-string data-str))

(defn- write-nippy-ln
  "Writes data as a base64 encoded line to buffered writer"
  [^BufferedWriter bwr data]
  (doto bwr
    (.write (->str data))
    (.newLine)))

(defn path->folder+filename [filepath]
  (let [ptrn (re-pattern File/separator)
        parts (s/split filepath ptrn)]
    [(s/join File/separator (butlast parts)) (last parts)]))

(defn serialized-db-filepath ^String [& {:keys [filename idx-id index-folder serialized-filename]}]
  (let [db-md5 (ndfile-md5 filename)
        [folder-path file-path] (path->folder+filename filename)
        nddbmeta-filename (or serialized-filename
                              (str (first (s/split file-path #"\."))
                                   "_" db-md5
                                   idx-id
                                   ".nddbmeta"))]
    (str (or index-folder folder-path)
         File/separator
         nddbmeta-filename)))

(defn serialize-db
  "nd-db metadata format v0.9.0+"
  ([db]
   {:pre [(ndut/db? db)]}
   (serialize-db db
                 @(:index db)
                 (serialized-db-filepath db))
   db)
  ([db-info index nddbmeta-filepath]
   {:pre [(map? db-info) (seqable? index) (string? nddbmeta-filepath)]}
   (with-open [w ^BufferedWriter (io/writer nddbmeta-filepath)]
     ;; writing to EDN string takes ~5x longer than using nippy+b64
     (write-nippy-ln w (dissoc db-info :index :as-of :id-fn :index-persist?))
     (doseq [part (partition-all 1000 (seq index))]
       (doseq [i part]
         (write-nippy-ln w (vec i)))
       (.flush ^BufferedWriter w)))))

(defn- maybe-update-filename [d filename]
  (let [{org-filename :filename} d]
    (if-not (= org-filename filename)
      (-> d
          (update-keys #(if (= :filename %) :org-filename %))
          (assoc :filename filename))
      d)))

(defn- _parse-db
  "Parse nd-db metadata format pre v0.9.0"
  [{:keys [filename]} serialized-filename]
  {:post [(ndut/db? %)]}
  (-> serialized-filename
      nippy/thaw-from-file
      (maybe-update-filename filename)
      (assoc :serialized-filename serialized-filename)))

(defn parse-db
  "Parse nd-db metadata format v0.9.0+"
  [{:keys [filename] :as params} serialized-filename]
  (try
    (with-open [r (io/reader ^String serialized-filename)]
      (let [[meta] (line-seq r)]
        (-> meta
            str->
            (assoc :index (delay (with-open [r2 (io/reader ^String serialized-filename)]
                                   (->> (line-seq r2)
                                        rest
                                        (map str->)
                                        (into {})))))
            (maybe-update-filename filename))))
    (catch Exception e
      (when (or (-> e ex-message (s/includes? "String.getBytes"))
                (s/includes? (ex-message e) "base64"))
        ;; fallback to pre v0.9.0 metadata standard
        (_parse-db params serialized-filename)))))

(defn append+newline
  "append to a file, return count of bytes appended"
  [^Writer writer]
  (fn [data]
    (let [data-str (str (->str data) "\n")]
      (doto writer
        (.write data-str)
        (.flush))
      (count data-str))))

(defn- infer-doctype [filename]
  (condp = (last (s/split filename #"\."))
    "ndnippy" :nippy
    "ndjson" :json
    "ndedn" :edn
    "csv" :csv
    "tsv" :tsv
    :unknown))

(defn parse-params
  "Parses input params for intake of raw-db"
  [& {:keys [filename
             id-fn id-rx-str
             id-path
             id-name id-type
             index-folder index-persist?] :as params}]
  {:pre [(or (fn? id-fn)
             (string? id-rx-str)
             (vector? id-path)
             (and id-name id-type)
             (or (nil? filename)
                 (string? filename)))]
   :post [#(and (:filename %)
                (:id-fn %)
                (:idx-id %)
                (:doc-type %))]}
  (let [doc-type (infer-doctype filename)]
    (when (and id-path (not= :nippy doc-type))
      (throw (ex-info "For performance reasons :id-path param is only allowed for .ndnippy files - recommended instead is explicity :id-fn with a regex (or :id-name and :id-type combo)" params)))

    (when (and id-name id-type (not= :json doc-type))
      (throw (ex-info "Right now use of :id-name and :id-type is only supported with .ndjson files. Recommend instead to use :id-fn with a regex directly, for .ndedn input" params)))

    (with-meta (assoc (merge (cond id-fn {:id-fn id-fn
                                          :idx-id ""}
                                   id-rx-str (ndid/rx-str->id+fn id-rx-str)
                                   id-path (ndid/pathy->id+fn id-path str->)
                                   :else (ndid/name-type->id+fn params))
                             (when index-folder {:index-folder index-folder}))
                      :doc-type doc-type
                      :filename filename
                      :index-persist? (not (false? index-persist?)))
      {:parsed? true})))

(defn mv-file [source target]
  (shell/sh "mv" source target))

(defn compressed-input-stream ^CompressorInputStream [filename]
  (let [in ^BufferedInputStream (io/input-stream filename)]
    (.createCompressorInputStream (CompressorStreamFactory.) in)))
