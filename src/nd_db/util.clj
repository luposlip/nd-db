(ns nd-db.util)

(defn str->hash [in]
  (.hashCode ^String in))

(defn db? [candidate]
  (boolean
   (and (future? candidate)
        (contains? @candidate :filename)
        (contains? @candidate :index))))
