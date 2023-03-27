(ns nd-db.csv-test
  (:require [nd-db.csv :as sut]
            [clojure.test :refer :all]))

(deftest col-str->key-vec
  (is (= [:a :b :c] (sut/col-str->key-vec #"," "a,B , C"))))
