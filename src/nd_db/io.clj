(ns nd-db.io
  (:require [clojure.java.io :as io]
            [clojure.string :as s]
            [taoensso.nippy :as nippy]
            digest
            [nd-db.util :as ndut])
  (:import [java.io File Writer BufferedWriter FileWriter]))

(defn- ndfile-md5
  "Reads first 10 lines of file, return corresponding MD5"
  [filename]
  (with-open [r (io/reader filename)]
    (let [input (take 10 (line-seq r))]
      (digest/md5 (s/join input)))))

(defn ->str ^String [data]
  (nippy/freeze-to-string data))

(defn str-> [data-str]
  (nippy/thaw-from-string ^String data-str))

(defn- write-nippy-ln
  "Writes data as a base64 encoded line to buffered writer"
  [^BufferedWriter bwr data]
  (doto bwr
    (.write (->str data))
    (.newLine)))

(defn serialize-db
  "nd-db metadata format v0.9.0+"
  [db]
  (with-open [bwr ^BufferedWriter (BufferedWriter.
                                   (FileWriter. ^String
                                                (:serialized-filename @db)))]
    ;; writing to EDN string takes ~5x longer than using nippy+b64
    (write-nippy-ln bwr (dissoc @db :index :id-fn :idx-id :index-persist?))
    (doseq [part (partition-all 1000 (seq (:index @db)))]
      (doseq [i part]
        (write-nippy-ln bwr (vec i)))
      (.flush bwr)))
  db)

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
  (future (-> serialized-filename
              nippy/thaw-from-file
              (maybe-update-filename filename))))

(defn parse-db
  "Parse nd-db metadata format v0.9.0+"
  [{:keys [filename] :as params} serialized-filename]
  (future
    (try
      (with-open [r (io/reader ^String serialized-filename)]
        (let [[meta & idx] (line-seq r)]
          (-> meta
              str->
              (assoc :index (->> idx
                                 (map str->)
                                 (into {})))
              (maybe-update-filename filename))))
      (catch Exception e
        (when (or (-> e ex-message (s/includes? "String.getBytes"))
                  (s/includes? (ex-message e) "base64"))
          ;; fallback to pre v0.9.0 metadata standard
          (deref
           (_parse-db params serialized-filename)))))))

(defn serialize-db-filename ^String [{:keys [filename idx-id index-folder]}]
  (let [db-filename (last (s/split filename (re-pattern File/separator)))
        db-md5 (ndfile-md5 filename)]
    (str (or index-folder (System/getProperty "java.io.tmpdir"))
         File/separator
         (first (s/split db-filename #"\."))
         "_" db-md5
         idx-id
         ".nddbmeta")))

(defn append+newline
  "append to a file, super simple lock mechanism"
  [^Writer writer]
  (fn [data]
    (let [data-str (str (->str data) "\n")]
      (doto writer
        (.write data-str)
        (.flush))
      (count data-str))))

(defn- name-type->id+fn
  "Generates valid :id-fn input based on :id-name and :id-type"
  [{:keys [id-name id-type source-type]
    :or {id-type :string}}]
  (when (string? id-name)
    {:idx-id (str id-name (name id-type))
     :id-fn (let [source-type (or source-type id-type)
                  source-pattern (condp = source-type
                                   :integer "(\\d+)"
                                   "\"(\\w+)\"")]
              (condp = id-type
                :integer #(BigInteger.
                           ^String
                           (second
                            (re-find
                             (re-pattern (format "%s\":%s" id-name source-pattern))
                             %)))
                #(second
                  (re-find
                   (re-pattern (format "%s\":%s" id-name source-pattern))
                   %))))}))

(defn- path->id+fn
  "Generates valid :id-fn input based on :id-path (.ndnippy only!)"
  [id-path]
  {:pre [(vector? id-path)]}
  {:idx-id (s/join (map #(if (keyword? %) (name %) %) id-path))
   :id-fn #(-> % str-> (get-in id-path))})

(defn- rx-str->id+fn
  "Generates valid :id-fn input based on a regular expression string"
  [rx-str]
  {:idx-id (ndut/str->hash rx-str)
   :id-fn #(Integer. ^String (second (re-find (re-pattern rx-str) %)))})

(defn- infer-doctype [filename]
  (condp = (last (s/split filename #"\."))
    "ndedn" :edn
    "ndjson" :json
    "ndnippy" :nippy
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
                                   id-rx-str (rx-str->id+fn id-rx-str)
                                   id-path (path->id+fn id-path)
                                   :else (name-type->id+fn params))
                             (when index-folder {:index-folder index-folder}))
                      :doc-type doc-type
                      :filename filename
                      :index-persist? (not (false? index-persist?)))
      {:parsed? true})))
