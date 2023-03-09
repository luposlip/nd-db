(ns nd-db.index
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.core.reducers :as r]
            [nd-db.util :as ndut])
  (:import [java.time Instant]
           [java.io BufferedReader FileReader]))

(defn index-id
  "This function generates a pseudo unique index ID for the combination
  of the ID function and the filename."
  [& {:keys [filename id-fn]}]
  (with-open [in (io/reader filename)]
    (mapv id-fn (take 10 (line-seq in)))))

(defn idx-reducr [id-fn]
  ;; TODO: Consider creating the [s l] sub vec immediately
  ;;       instead of in the last reduce for each batch.
  ;;       -> potentially better performance (especially because the
  ;;          last reduce is in the main thread)
  (fn [acc line]
    (let [len (count (.getBytes ^String line))
          id (id-fn line)
          [_ start plen] (or (peek acc) [nil -1 0])]
      (conj acc [id (+ 1 start plen) len]))))

(defn idx-combinr
  ([] [])
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
      ;; without parallelization: 18s
      ;; with: 6s (partition size 2048, fold size 32
      ;; that's around 2/3 less processing time
      (with-meta
        (->> rdr
             line-seq ;; for parallel processing, enable line below!
             (partition-all 2048)
             (reduce
              (fn [[offset _ :as acc] part]
                (let [res (->> part
                               (into [])
                               (r/fold 32
                                       idx-combinr
                                       (idx-reducr id-fn)))
                      [s l] (->> res peek rest)]
                  (-> acc
                      (update 0 #(+ 1 % s l))
                      (update 1 merge (reduce
                                       (fn [a [id s l]]
                                         (assoc a id [(+ offset s) l]))
                                       {} res)))))
              [0 {}])
             second)
        {:timestamp timestamp}))))

(defn reader
  "Returns a BufferedReader of the database index.
   Use this in a with-open block (or close it explicitly when done)!"
  ^BufferedReader [db]
  {:post [(instance? BufferedReader %)]}
  (when-not (and
             (ndut/v090+? db)
             (ndut/nippy-db? db))
    (throw (ex-info "ERROR: pre v0.9.0 .nddbmeta format - cannot lazily traverse index.
Consider converting the index (or delete it, which will auto-recreate it)."
                    @db)))
  (let [r (BufferedReader. (FileReader. ^String (:serialized-filename @db)))]
    (.readLine r) ;; first line isn't part of the index
    r))
