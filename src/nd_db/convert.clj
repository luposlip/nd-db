(ns nd-db.convert
  (:require [clojure.java.io :as jio]
            [clojure.core.reducers :as r]
            [nd-db
             [core :as nddb]
             [io :as ndio]
             [util :as util]]))

(defn ->ndnippy
  "Converts .ndjson and .ndedn files to .ndnippy.
  
  .ndnippy is MUCH faster (10-15x) and requires less memory to process.

  Also the resulting database size is much smaller:
  ~27GB .ndedn becomes ~12GB .ndnippy (~65% reduction!)"
  [in-db out-filename]
  {:pre [(util/db? in-db)]}
  (with-open [writer (jio/writer out-filename)]
    (->> @in-db
         :index
         keys
         (into [])
         (r/map (partial nddb/q in-db))
         (r/map (ndio/append+newline writer))
         (r/fold 50 r/cat r/append!)
         count)))
