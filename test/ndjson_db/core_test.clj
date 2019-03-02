(ns ndjson-db.core-test
  (:require [clojure.test :refer :all]
            [ndjson-db.core :as db]))

(deftest ndjson-idx*
  (testing ".ndjson file transformed to random access index" 
    (is (= {1 [0 49]
            222 [50 22]
            333333 [73 46]}
           (db/ndjson->idx* "resources/test/test.ndjson")))))

(deftest query-by-id
  (testing ".ndjson file as random access database for single id"
    (is (= {:id 222
            :data 42}
           (db/query-by-id "resources/test/test.ndjson" 222)))))

(deftest ndjson-query
  (testing ".ndjson file as random access database for multiple ids"
    (is (= [{:id 333333
             :data {:datakey "datavalue"}}
            {:id 1
             :data ["some" "semi-random" "data"]}]
           (into []
                 (db/ndjson-query "resources/test/test.ndjson" [333333 1 77]))))))

;; To test with real DB:
(comment
  (time 
   (doall
    (map :id
         (db/ndjson-query
          "resources/test/database.ndjson"
          [19956032 11 18786787 22])))))
