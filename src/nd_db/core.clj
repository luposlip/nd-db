(ns nd-db.core
  (:require [clojure.core.reducers :as r]
            [clojure.string :as s]
            [clojure.java.io :as jio]
            [clojure.edn :as edn]
            [cheshire.core :as json])
  (:import [java.util Date]))

(def index-fns
  "In the form {\"filename1\"
                   {\"9298cvoa\" {index-w-first-few-ids-as-str-=-9298cvoa}
                \"filename2\"
                   {\"ao3oijf8\" {index-w-first-few-ids-as-str-=-ao3oijf8}}}"
  (atom {}))

(defn get-id-fn [{:keys [id-name id-type source-type]}]
  {:pre [(string? id-name)]}
  (let [source-type (or source-type id-type)
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
         %)))))

(defn reducr [id-fn]
  (fn [a line]
    (let [len (count (.getBytes ^String line))
          id (id-fn line)
          idx (first a)]
      (-> a
          (update 0 + (inc len))
          (update 1 assoc id [idx len])))))

(defn create-index
  "Builds up an index of Entity IDs as keys (IDs extracted with id-fn),
  and as value a vector with 2 values:
  - the start index in the text file to start read EDN for the input doc
  - the length in bytes input doc"
  [filename idx-id]
  {:pre [(string? filename)
         (vector? idx-id)]}
  (let [id-fn (get-in @index-fns [filename idx-id])]
    (if (fn? id-fn)
      (with-open [rdr (jio/reader filename)]
        (->> rdr
             line-seq
             (r/fold 512 #(vector 0 {}) (reducr id-fn))
             second))
      (throw (ex-info "No id-fn found for index" {:filename filename
                                                  :idx-id idx-id})))))

(defn index-id
  "This function generates a pseudo unique index ID for the combination
  of the ID function and the filename."
  [{:keys [id-fn filename id-name id-type] :as params}]
  (with-open [in (clojure.java.io/reader filename)]
    (into []
          (map (or id-fn (get-id-fn params))
               (take 5
                     (line-seq in))))))

(defn db? [candidate]
  (and (future? candidate)
       (contains? @candidate :filename)
       (contains? @candidate :index)))

(defn infer-doctype [filename]
  (condp = (last (s/split filename #"\."))
    "ndedn" :edn
    :json))

(defn db
  "Creates a database var which can be used to perform queries"
  [{:keys [id-fn id-name id-type doc-type filename] :or {id-type :string} :as params}]
  {:pre [(or (string? id-name)
             (fn? id-fn))
         (string? filename)]}
  (let [doc-type (or (#{:json :edn} doc-type)
                     (infer-doctype filename))
        id-fn (if id-name
                (get-id-fn params)
                id-fn)
        idx-id (index-id params)]
    (when (not (get-in @index-fns [filename idx-id]))
      (swap! index-fns assoc-in [filename idx-id] id-fn))
    (future {:filename filename
             :index (create-index filename idx-id)
             :doc-type (or doc-type :json)
             :timestamp (Date.)})))

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

(defn parse-doc [db doc-str]
  (condp = (:doc-type @db)
    :json (json/parse-string doc-str true)
    :edn (edn/read-string doc-str)
    (throw (ex-info "Unknown doc-type" {:doc-type (:doc-type @db)}))))

(defmethod q :single query-single
  [db id]
  {:pre [(db? db)
         (not (nil? id))]}
  (let [[start len] (get (:index @db) id)
        bytes (byte-array len)]
    (when (and start len)
      (doto (java.io.RandomAccessFile. (:filename @db) "r")
        (.seek start)
        (.read bytes 0 len)
        (.close))
      (->> bytes
           (String.)
           (parse-doc db)))))

(defmethod q :sequential query-multiple
  [db ids]
  {:pre [(db? db)
         (sequential? ids)]}
  (keep (partial q db) ids))

