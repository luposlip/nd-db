(ns nd-db.csv
  (:require [clojure.string :as s]))

(defn csv-row->data [& {:keys [cols separator drop-n skip-last? col-parser]
                        :or {drop-n 0 col-parser identity}}]
  (fn [row]
    (zipmap cols
            (->> (s/split row (re-pattern (or separator ",")))
                 (drop drop-n)
                 (#(if skip-last?
                     (butlast %)
                     %))
                 (map col-parser)))))
