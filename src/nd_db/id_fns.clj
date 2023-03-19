(ns nd-db.id-fns
  (:require [clojure
             [edn :as edn]
             [string :as s]]
            [clojure.java.io :as io]
            [nd-db.util :as ndut]))

(defn name-type->id+fn
  "Generates valid :id-fn input based on :id-name, :id-type and
   optionally :source-type"
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

(defmulti pathy->id+fn
  "Generates valid :id-fn input based on :id-path or :id keyword.
   Optionally takes a parser - defaults to parsing ndnippy."
  (fn [idy & [parser]]
    (if (vector? idy)
      :vector
      :key)))

(defmethod pathy->id+fn :vector
  [id-path parser]
  {:idx-id (s/join (map #(if (keyword? %) (name %) %) id-path))
   :id-fn #(get-in (parser %) id-path)})

(defmethod pathy->id+fn :key
  [k parser]
  {:idx-id (if (keyword? k) (name k) (str k))
   :id-fn #(get (parser %) k)})


(defn rx-str->id+fn
  "Generates valid :id-fn input based on a regular expression string"
  [rx-str]
  {:idx-id (ndut/str->hash rx-str)
   :id-fn #(Integer. ^String (second (re-find (re-pattern rx-str) %)))})

(defn- default-col-parser [col-str]
  (if (ndut/number-str? col-str)
    (edn/read-string col-str)
    col-str))

(defn csv-id+fn [& {:keys [filename col-separator id-path col-parser]
                    :or {col-parser default-col-parser}}]
  {:pre [(string? filename)
         (string? col-separator)
         (keyword? id-path)]}
  (let [ptrn (re-pattern col-separator)
        col-str (with-open [r (io/reader filename)]
                  (-> r line-seq first))
        cols (ndut/col-str->key-vec ptrn col-str)
        id-col-idx (ndut/index-of id-path cols)]
    (fn [row-str]
      (col-parser (nth (s/split row-str ptrn) id-col-idx)))))
