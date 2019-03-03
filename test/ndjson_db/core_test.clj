(ns ndjson-db.core-test
  (:require [clojure.test :refer :all]
            [ndjson-db.core :as db]))

(deftest ndjson-idx*
  ;; Hack
  (swap! db/id-fns assoc
         :by-id  #(Integer. ^String (second (re-find #"^\{\"id\":(\d+)" %))))
  (testing ".ndjson file transformed to random access index" 
    (is (= {1 [0 49]
            222 [50 22]
            333333 [73 46]}
           (db/ndjson->idx* :by-id 
                            "resources/test/test.ndjson")))))

(deftest query-by-id
  ;; Hack
  (swap! db/id-fns assoc
         :by-id  #(Integer. ^String (second (re-find #"^\{\"id\":(\d+)" %))))
  (testing ".ndjson file as random access database for single id" 
    (is (= {:id 222
            :data 42}
           (db/query-single :by-id
                            "resources/test/test.ndjson"
                            222)))))

(deftest ndjson-query
  (testing ".ndjson file as random access database for multiple ids"
    (is (= [{:id 333333
             :data {:datakey "datavalue"}}
            {:id 1
             :data ["some" "semi-random" "data"]}]
           (into []
                 (db/query {:id-fn-key :by-id
                            :id-fn #(Integer. ^String (second (re-find #"^\{\"id\":(\d+)" %)))
                            :filename  "resources/test/test.ndjson"}
                           [333333 1 77]))))))

;; To test with real DB, download all verified Twitter users
;; from here:
;; https://files.pushshift.io/twitter/TU_verified.ndjson.xz
;;
;; Then put the file in resources/test/TU_verified.ndjson, and
;; run the following in a repl:
;;
#_(time 
   (def katy-gaga-and-gates
     (doall
      (db/query
       {:id-fn-key :by-screen_name-lowercase
        :id-fn #(->> %
                     (re-find #"\"screen_name\":\"(\w+)\"")
                     second
                     clojure.string/lower-case)
        :filename "resources/TU_verified.ndjson"}
       ["katyperry" "ladygaga" "billgates" "bymikewilson"]))))

;; The extracted .ndjson files is 513 MB (297,878 records).
;; 
;; On my laptop (Intel® Core™ i7-8750H CPU @ 2.20GHz × 6 cores with 31,2 GB RAM)
;; the initial build of the index takes around 3 seconds, and the subsequent
;; query of the above 3 verified Twitter users takes around 1 millisecond
