(ns nd-db.csv-test
  (:require [nd-db.csv :as sut]
            [clojure.test :refer :all]))

(deftest col-str->key-vec
  (is (= [:a :b :c] (sut/col-str->key-vec #"," "a,B , C"))))

(deftest csv-row->data
  (let [f (sut/csv-row->data {:cols "a,c" :col-separator ","})]
    (is (= {:a "b" :c "d"} (f "b,d")))
    (is (= {:a "d" :c "b"} (f "d,b")))))

(deftest data->csv-row
  (let [f (sut/data->csv-row {:cols "a,c" :col-separator ","})]
    (is (= "b,d" (f {:a "b" :c "d"})))
    (is (= "b,d" (f {:c "d" :a "b"})))))
