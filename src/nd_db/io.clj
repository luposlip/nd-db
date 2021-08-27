(ns nd-db.io
  (:require [clojure.java.io :as io]
            [clojure.string :as s]
            [taoensso.nippy :as nippy]
            [buddy.core.codecs :as c]
            digest
            [nd-db.util :as u])
  (:import [java.io Writer]))

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
  {:pre [(u/db? db)]}
  (with-open [os (io/output-stream filename)]
    (.write os (nippy/freeze @db)))
  db)

(defn parse-db [filename]
  {:post [(u/db? %)]}
  (future (nippy/thaw-from-file filename)))

(defn serialize-db-filename [{:keys [filename id-rx-str]}]
  (let [db-filename (last (s/split filename #"/"))
        db-md5 (ndfile-md5 filename)]
    (str "/tmp/"
         (first (s/split db-filename #"\."))
         "_" db-md5
         (if id-rx-str (str "_" (u/str->hash id-rx-str)) "")
         ".nddbmeta")))

(defn append+newline
  "append to a file, super simple lock mechanism"
  [^Writer writer]
  (fn [data]
    (doto writer
      (.write (str (->str data) "\n"))
      (.flush))))
