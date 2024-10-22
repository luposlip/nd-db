(ns nd-db.zip-index
  (:require [clojure.java.io :as io]
            [clarch.core :as clarch])
  (:import [java.time Instant]))

(defn zip-entry-meta->idx-reducer [id-fn]
  (fn [a {:keys [offset compressed-size method]
          :as i}]
    (assoc a
           (id-fn i)
           [offset compressed-size method])))

(defn zf->id-fn [zf & {:keys [id-fn deflate]
                       :or {deflate true}}]
  (fn [entry]
    (cond->> entry
      (true? deflate) (clarch/zip-entry-meta->uncompressed-bytes zf)
      true id-fn)))

(defn zip-index
  "Creates an index over the content of a zip file.
  id-fn takes the unzipped bytes of the zip entry as parameter, and returns
  the unique entry ID.
  Deflates per default. Add flag :deflate false to inhibit.
  Inhibit deflation and use :filename as id-fn to use zip entry filename as ID.
  Index is (as always) returned as a map"
  [& {:keys [filename id-fn] :as opts}]
  {:pre [(ifn? id-fn)]}
  (let [zip-file (io/file filename)
        zems (clarch/zip-entry-metas zip-file)]
    (with-meta
      (reduce (zip-entry-meta->idx-reducer
               (zf->id-fn zip-file opts))
              {}
              zems)
      {:as-of (Instant/now)})))

#_
(zip-index :filename "resources/test/jsons.zip"
           :id-fn #(-> %
                       (charred.api/read-json :key-fn keyword)
                       (get :id)))
#_
(zip-index :filename "resources/test/edns.zip"
           :id-fn (fn [d]
                    (->> d
                         (#(String. ^"[B" %))
                         (re-find #":id (\d+)")
                         second
                         Long/parseLong)))
