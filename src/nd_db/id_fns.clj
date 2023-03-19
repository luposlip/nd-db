(ns nd-db.id-fns
  (:require [clojure.string :as s]
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
