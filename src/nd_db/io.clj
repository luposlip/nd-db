(ns nd-db.io
  (:require [clojure
             [string :as str]
             [edn :as edn]]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [cheshire.core :as json]
            [taoensso.nippy :as nippy]
            digest
            [nd-db
             [util :as ndut]
             [io :as ndio]
             [id-fns :as ndid]
             [csv :as ndcs]])
  (:import [java.io File Writer FileWriter BufferedWriter
            RandomAccessFile]))

(defn tmpdir []
  (System/getProperty "java.io.tmpdir"))

(defn last-line [filename]
  (let [file (io/file filename)
        radfile (RandomAccessFile. file "r")
        newline (byte \newline)]
    (loop [pointer (dec (.length file))
           bytes []
           first true]
      (let [byte (do (.seek radfile pointer) (.read radfile))]
        (if (and (false? first) (= newline byte) (< 0 (count bytes)))
          (->> bytes reverse (map char) (apply str))
          (recur (dec pointer) (if (true? first)
                                 bytes
                                 (conj bytes byte))
                 false))))))

(defn ndfile-md5
  "Reads first and last line of file, return corresponding MD5"
  [filename]
  (with-open [r (io/reader filename)]
    (-> r line-seq first digest/md5)))

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

(defn path->folder+filename
  "Splits filename into path/folder and filename"
  [filepath]
  (let [ptrn (re-pattern File/separator)
        parts (str/split filepath ptrn)]
    [(str/join File/separator (butlast parts)) (last parts)]))

(defn serialized-db-filepath ^String [& {:keys [filename idx-id index-folder serialized-filename]}]
  (let [[folder-path file-path] (path->folder+filename filename)
        nddbmeta-filename (or serialized-filename
                              (str (first (str/split file-path #"\."))
                                   "_" (ndfile-md5 filename)
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
     (write-nippy-ln w (dissoc db-info
                               :index :as-of
                               :id-fn :col-parser :doc-parser :doc-emitter
                               :index-persist?))
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

(defn params->doc-parser [{:keys [doc-type] :as params}]
  (case doc-type
    :json #(json/parse-string % true)
    :edn edn/read-string
    :nippy ndio/str->
    :csv (ndcs/csv-row->data params)
    :else (throw (ex-info "Unknown doc-type" {:doc-type doc-type}))))

(defn params->doc-emitter [{:keys [doc-type] :as params}]
  (case doc-type
    :json json/encode
    :edn str
    :nippy ndio/->str
    :csv (ndcs/data->csv-row params)
    :else (throw (ex-info "Unknown doc-type" {:doc-type doc-type}))))

(defn- ^{:deprecated "v0.9.0"} _parse-db
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
                                        (into {}))))
                   :doc-parser (params->doc-parser params)
                   :doc-emitter (params->doc-emitter params))
            (maybe-update-filename filename))))
    (catch Exception e
      (when (or (-> e ex-message (str/includes? "String.getBytes"))
                (str/includes? (ex-message e) "base64"))
        ;; fallback to pre v0.9.0 metadata standard
        (_parse-db params serialized-filename)))))

(defn append+newline
  "Append to a ndnippy file, return count of bytes appended"
  [^Writer writer]
  (fn [data]
    (let [data-str (str (->str data) "\n")]
      (doto writer
        (.write data-str)
        (.flush))
      (count data-str))))

(defn- infer-doctype [filename]
  (condp = (last (str/split filename #"\."))
    "ndnippy" :nippy
    "ndjson" :json
    "ndedn" :edn
    "csv" :csv
    "tsv" :tsv
    :unknown))

(defn parse-params
  "Parses input params for intake by raw-db"
  [& {:keys [filename
             id-fn id-rx-str
             id-path
             id-name id-type
             col-separator col-parser ;; CSV
             index-folder index-persist?] :as params}]
  {:pre [(string? filename)
         (or (fn? id-fn)
             (string? id-rx-str)
             id-path ;; vector or single val
             (and id-name id-type)
             (and (string? col-separator) (keyword? id-path))
             ((some-fn nil? ifn?) col-parser))]
   :post [#(and (:filename %)
                (:id-fn %)
                (:idx-id %)
                (:doc-type %)
                (:doc-parser %)
                (:doc-emitter %))]}
  (let [doc-type (infer-doctype filename)]
    (when (and id-path (and (not col-separator)
                            (not= :nippy doc-type)))
      (throw (ex-info "For performance reasons :id-path param is only allowed for .ndnippy files - recommended instead is explicity :id-fn with a regex (or :id-name and :id-type combo)" params)))

    (when (and id-name id-type (not= :json doc-type))
      (throw (ex-info "Right now use of :id-name and :id-type is only supported with .ndjson files. Recommend instead to use :id-fn with a regex directly, for .ndedn input" params)))

    (let [parsed (cond-> (cond id-fn {:id-fn id-fn
                                      :idx-id ""}
                               id-rx-str (ndid/rx-str->id+fn id-rx-str)
                               (and col-separator id-path)
                               (ndid/csv-id+fn params)
                               id-path (ndid/pathy->id+fn id-path str->)
                               :else (ndid/name-type->id+fn params))
                   true (merge
                         (when index-folder {:index-folder index-folder}))
                   true (assoc
                         :doc-type doc-type
                         :filename filename
                         :index-persist? (not (false? index-persist?)))
                   (= :csv doc-type)
                   (assoc :col-separator col-separator
                          :id-path id-path
                          :cols (with-open [r (io/reader filename)]
                                  (ndcs/col-str->key-vec
                                   (re-pattern col-separator)
                                   (first (line-seq r))))))]
      (with-meta (-> parsed
                     (assoc :doc-parser (params->doc-parser parsed))
                     (assoc :doc-emitter (params->doc-emitter parsed)))
        {:parsed? true}))))

(defn mv-file [source target]
  (shell/sh "mv" source target))

(defn ^BufferedWriter append-writer
  "Return af BufferedWriter for the database index.
   Use in a with-open block or close explicitly."
  [^String filename & _]
  {:pre [(string? filename)]
   :post [(instance? BufferedWriter %)]}
  (BufferedWriter. (FileWriter. (io/file filename) true)))
