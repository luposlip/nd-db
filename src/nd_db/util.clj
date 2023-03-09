(ns nd-db.util)

(defn str->hash [in]
  (.hashCode ^String in))

(defn db? [candidate]
  (boolean
   (and (future? candidate)
        (contains? @candidate :filename)
        (contains? @candidate :index))))

(defn v090+? [candidate]
  (:version @candidate))

(defn nippy-db? [candidate]
  (= :nippy (:doc-type @candidate)))
