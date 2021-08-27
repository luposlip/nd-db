(ns nd-db.util)

(defn str->hash [in]
  (.hashCode ^String in))

(defn db? [candidate]
  (and (future? candidate)
       (contains? @candidate :filename)
       (contains? @candidate :index)))

(defn name-type->id+fn [{:keys [id-name id-type source-type]
                         :or {id-type :string}}]
  {:pre [(string? id-name)]}
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
                 %))))})

(defn rx-str->id+fn
  "Generates "[rx-str]
  {:idx-id (str->hash rx-str)
   :id-fn #(Integer. ^String
                     (second (re-find (re-pattern rx-str) %)))})
