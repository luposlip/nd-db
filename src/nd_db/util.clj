(ns nd-db.util)

(defn str->hash [in]
  (.hashCode ^String in))

(defn v090+? [candidate]
  (:version @candidate))

(defn db? [candidate]
  (boolean
   (and (future? candidate)
        (or
         (v090+? candidate)
         (and
          (contains? @candidate :filename)
          (contains? @candidate :index))))))

(defn nippy-db? [candidate]
  (= :nippy (:doc-type @candidate)))
