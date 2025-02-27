(ns nd-db.secondary-index
  (:require [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [nd-db
             [util :as ndut]
             [io :as ndio]
             [core :as core]]
            [clojure.data.avl :as avl])
  (:import [java.io File]))

(defn- extract-secondary-keys
  "Extract secondary key values from a document using extractor fns"
  [extractors doc]
  (reduce-kv
   (fn [acc k extractor]
     (assoc acc k (extractor doc)))
   {}
   extractors))

(defn- build-secondary-index
  "Build a secondary index for a specific field"
  [docs extractor-fn range?]
  (let [index-type (if range? avl/sorted-map {})]
    (reduce
     (fn [idx doc]
       (let [key-val (extractor-fn doc)
             doc-id (get doc "id")] ;; Assuming "id" is the primary key field
         (if key-val
           (update idx key-val (fnil conj #{}) doc-id)
           idx)))
     index-type
     docs)))

(defn create-secondary-indexes
  "Create secondary indexes for the given documents
   extractors-map is a map of index name to extractor function
   range-indexes is a set of index names that should support range queries"
  [docs extractors-map range-indexes]
  (reduce-kv
   (fn [indexes idx-name extractor]
     (let [range? (contains? range-indexes idx-name)]
       (assoc indexes idx-name
              (build-secondary-index docs extractor range?))))
   {}
   extractors-map))

(defn q-by
  "Query documents by a secondary index field"
  [db index-name value]
  (when-let [sec-indexes (:secondary-indexes db)]
    (when-let [sec-index (get sec-indexes index-name)]
      (when-let [doc-ids (get sec-index value)]
        (core/q db (vec doc-ids))))))

(defn q-where
  "Query documents matching multiple criteria
   criteria is a map of index names to values"
  [db criteria]
  (when-let [sec-indexes (:secondary-indexes db)]
    ;; Find matching doc-ids for each criterion
    (let [matches (for [[idx-name value] criteria
                       :let [sec-index (get sec-indexes idx-name)]
                       :when sec-index
                       :let [matching-ids (get sec-index value)]
                       :when matching-ids]
                   matching-ids)
          ;; Find intersection of all matches
          common-ids (when (seq matches)
                      (reduce clojure.set/intersection matches))]
      (when (seq common-ids)
        (core/q db (vec common-ids))))))

(defn q-range
  "Query documents by a range of values
   Requires the index to be a sorted index (created with range? = true)"
  [db index-name min-val max-val]
  (when-let [sec-indexes (:secondary-indexes db)]
    (when-let [sec-index (get sec-indexes index-name)]
      (when (instance? clojure.lang.Sorted sec-index)
        (let [matching-entries (subseq sec-index >= min-val <= max-val)
              all-doc-ids (reduce
                          (fn [ids [_ doc-id-set]]
                            (clojure.set/union ids doc-id-set))
                          #{}
                          matching-entries)]
          (when (seq all-doc-ids)
            (core/q db (vec all-doc-ids))))))))

