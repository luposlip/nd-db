(ns nd-db.util)

(defn- num-chars* []
  (->> (range 48 58)
       (map char)
       (cons \.)
       (cons \,)
       set))

(def ^:private num-chars (memoize num-chars*))

(defn str->hash [in]
  (.hashCode ^String in))

(defn db? [candidate]
  (boolean
   (and (map? candidate)
        (contains? candidate :filename)
        (contains? candidate :index))))

(defn v090+? [candidate]
  (:version candidate))

(defn nippy-db? [candidate]
  (= :nippy (:doc-type candidate)))

(defn number-str?
  "Takes a string, returns true if all chars are numeric.
   Accepts punctuation (dot and comma).

   Doesn't allow the number to begin with 0 followed by
   another number (such as 008)."
  [i]
  {:pre [(string? i)]}
  (loop [chars (seq i)
         leading-zero? nil]
    (if (seq chars)
      (if-let [fc (num-chars (first chars))]
        (cond (or (false? leading-zero?)
                  (and (nil? leading-zero?)
                       (not= fc \0)))
              (recur (rest chars) false)
              (and (nil? leading-zero?) (= fc \0))
              (recur (rest chars) true)
              :else false)
        false)
      true)))
