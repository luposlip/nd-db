(ns nd-db.zip-index
  (:require [clojure.java.io :as io]
            [clarch.core :as clarch]
            [charred.api :as charred]))

(defn zip-entry-meta->idx-reducer [id-fn]
  (fn [a {:keys [offset compressed-size] :as i}]
    (assoc a
           (id-fn i)
           [offset compressed-size])))

(defn zf->id-fn [zf inner-fn & {:keys [deflate] :or {deflate true}}]
  (fn [entry]
    (cond->> entry
      (true? deflate) (clarch/deflated-bytes zf)
      true inner-fn)))

(defn zip-index
  "Creates an index over the content of a zip file.
  id-fn takes the unzipped bytes of the zip entry as parameter, and should
  the unique entry ID.
  If this should just be the filename, just write :filename
  Deflates per default. Add flag :deflate false to inhibit.
  Inhibit deflation and use :filename as id-fn to use zip entry filename as key.
  Index is (as always) returned as a map"
  [filename id-fn & {:as opts}]
  {:pre [(ifn? id-fn)]}
  (let [f (io/file filename)
        zems (clarch/zip-entry-metas f)]
    (->> zems
         (take 2)
         (reduce (zip-entry-meta->idx-reducer
                  (zf->id-fn f id-fn opts))
                 {}))))

#_
(zip-index "/path/to/some-jsons.zip"
           #(-> %
                (charred/read-json :key-fn keyword)
                (get-in [:path :to :unique-id])))
