(ns nd-db.io
  (:require [clojure.java.io :as io]
            [clojure.string :as s]
            [taoensso.nippy :as nippy]
            [buddy.core.codecs :as c]
            digest
            [nd-db.util :as ndut])
  (:import [java.io File Writer]))

(defn ndfile-md5
  "Reads first 10 lines of file, return corresponding MD5"
  [filename]
  (with-open [r (io/reader filename)]
    (let [input (take 10 (line-seq r))]
      (digest/md5 (s/join input)))))

(defn ->str [data]
  (-> data nippy/freeze c/bytes->b64 c/bytes->str))

(defn str-> [data-str]
  (-> data-str c/str->bytes c/b64->bytes nippy/thaw))

(defn serialize-db [filename db]
  {:pre [(ndut/db? db)]}
  (with-open [os (io/output-stream filename)]
    (.write os (nippy/freeze @db)))
  db)

(defn parse-db [filename]
  {:post [(ndut/db? %)]}
  (future (nippy/thaw-from-file filename)))

(defn serialize-db-filename [{:keys [filename idx-id index-folder]}]
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

(defn name-type->id+fn
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

(defn path->id+fn
  "Generates valid :id-fn input based on :id-path (.ndnippy only!)"
  [id-path]
  {:pre [(vector? id-path)]}
  {:idx-id (s/join (map #(if (keyword? %) (name %) %) id-path))
   :id-fn #(-> % str-> (get-in id-path))})

(defn rx-str->id+fn
  "Generates valid :id-fn input based on a regular expression string"
  [rx-str]
  {:idx-id (ndut/str->hash rx-str)
   :id-fn #(Integer. ^String (second (re-find (re-pattern rx-str) %)))})

(defn parse-params
  "Parses input params for intake of raw-db"
  [{:keys [filename
           id-fn id-rx-str
           id-path
           id-name id-type
           index-folder index-persist?] :as params}]
  {:pre [(or (fn? id-fn)
             (string? id-rx-str)
             (vector? id-path)
             (and id-name id-type))]
   :post [#(and (:filename %)
                (:id-fn %)
                (:idx-id %))]}
  (with-meta (assoc (merge (cond id-fn {:id-fn id-fn
                                        :idx-id ""}
                                 id-rx-str (rx-str->id+fn id-rx-str)
                                 id-path (path->id+fn id-path)
                                 :else (name-type->id+fn params))
                           (when index-folder {:index-folder index-folder}))
                    :filename filename
                    :index-persist? (not (false? index-persist?)))
    {:parsed? true}))
