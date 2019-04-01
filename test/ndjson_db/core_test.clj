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

(deftest query-single
  ;; Hack
  (swap! db/id-fns assoc
         :by-id  #(Integer. ^String (second (re-find #"^\{\"id\":(\d+)" %))))
  (testing ".ndjson file as random access database for single id" 
    (is (= {:id 222
            :data 42}
           (db/query-single {:id-fn-key :by-id
                             :filename 
                             "resources/test/test.ndjson"}
                            222)))))

(deftest id-key-fn-by-name
  (testing "Generating map with :id-fn and :id-fn-key from json name"
    (let [{:keys [id-fn-key id-fn]} (db/id-key-fn-by-name "id" :integer)]
      (is (= :by-name-id id-fn-key))
      (is (fn? id-fn)))))

(deftest query
  (testing ".ndjson file as random access database for multiple ids"

    (testing "using :id-fn-key and :id-fn as params"
      (is (= [{:id 333333
               :data {:datakey "datavalue"}}
              {:id 1
               :data ["some" "semi-random" "data"]}]
             (into []
                   (db/query {:id-fn-key :by-id
                              :id-fn #(Integer. ^String (second (re-find #"^\{\"id\":(\d+)" %)))
                              :filename  "resources/test/test.ndjson"}
                             [333333 1 77])))))
    
    (testing "using :id-name and :id-type as params"
      (is (= [{:id 333333
               :data {:datakey "datavalue"}}
              {:id 1
               :data ["some" "semi-random" "data"]}]
             (into []
                   (db/query {:id-name "id"
                              :id-type :integer
                              :filename  "resources/test/test.ndjson"}
                             [333333 1 77])))))))

