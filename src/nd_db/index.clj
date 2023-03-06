(ns nd-db.index
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.core.reducers :as r])
  (:import [java.time Instant]))

(defn index-id
  "This function generates a pseudo unique index ID for the combination
  of the ID function and the filename."
  [& {:keys [filename id-fn]}]
  (with-open [in (io/reader filename)]
    (mapv id-fn (take 10 (line-seq in)))))

(defn idx-reducr [id-fn]
  (fn [acc line]
    (let [len (count (.getBytes ^String line))
          id (id-fn line)
          [_ start plen] (or (peek acc) [nil -1 0])]
      ;; TODO concat into list for parallelization
      (conj acc [id (+ 1 start plen) len]))))

(defn idx-combinr
  ([] []) ;; TODO: Lazify for parallelization
  ([_] [])
  ([acc more]
   (let [[_ prev-start prev-len] (or (peek acc) [nil -1 0])
         prev-offset (+ 1 prev-start prev-len)]
     (reduce
      (fn [a [id old-start len]]
        (conj a [id (+ prev-offset old-start) len]))
      acc
      more))))

(defn create-index
  "Builds up an index of Entity IDs as keys (IDs extracted with id-fn),
  and as value a vector with 2 values:
  - the start index in the text file to start read EDN for the input doc
  - the length in bytes input doc"
  [filename id-fn]
  {:pre [(string? filename)
         (fn? id-fn)]
   :post [(-> % meta :timestamp inst?)]}
  (with-open [rdr (io/reader filename)]
    (let [timestamp (Instant/now)]
     (->> rdr
          line-seq ;; for parallel processing, enable line below!
          ;;(into [])
          (r/fold (or (some-> (System/getenv "NDDB_LINES_PER_CORE")
                              edn/read-string)
                      512)
                  idx-combinr
                  (idx-reducr id-fn))
          (reduce
           (fn [acc i]
             (assoc acc (first i) (into [] (rest i))))
           (with-meta {} {:timestamp timestamp}))))))
