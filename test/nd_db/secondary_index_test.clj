(ns nd-db.secondary-index-test
  (:require [clojure.test :refer :all]
            [nd-db.core :as nddb]
            [nd-db.secondary-index :as sidx]))

(deftest secondary-index-test
  (testing "Creating and querying with secondary indexes"
    (let [test-db (nddb/db {:id-name "id"
                            :id-type :integer
                            :filename "resources/test/test.ndjson"
                            :secondary-indexes {:data-type (fn [doc] 
                                                            (cond 
                                                              (vector? (get doc "data")) :vector
                                                              (number? (get doc "data")) :number
                                                              (map? (get doc "data")) :map
                                                              :else :unknown))
                                                :has-datakey? (fn [doc]
                                                               (contains? (get doc "data") "datakey"))}})
          vector-docs (sidx/q-by test-db :data-type :vector)
          number-docs (sidx/q-by test-db :data-type :number)
          map-docs (sidx/q-by test-db :data-type :map)
          datakey-docs (sidx/q-by test-db :has-datakey? true)
          multi-criteria (sidx/q-where test-db {:data-type :map :has-datakey? true})]
      
      (is (= 1 (count vector-docs)))
      (is (= 1 (get (first vector-docs) "id")))
      
      (is (= 1 (count number-docs)))
      (is (= 222 (get (first number-docs) "id")))
      
      (is (= 1 (count map-docs)))
      (is (= 333333 (get (first map-docs) "id")))
      
      (is (= 1 (count datakey-docs)))
      (is (= 333333 (get (first datakey-docs) "id")))
      
      (is (= 1 (count multi-criteria)))
      (is (= 333333 (get (first multi-criteria) "id")))))
  
  (testing "Range queries with secondary indexes"
    (let [test-db (nddb/db {:id-name "id"
                            :id-type :integer
                            :filename "resources/test/test.ndjson"
                            :secondary-indexes {:id-value (fn [doc] (get doc "id"))}
                            :range-indexes #{:id-value}})
          range-docs (sidx/q-range test-db :id-value 200 300000)]
      
      (is (= 2 (count range-docs)))
      (is (contains? (set (map #(get % "id") range-docs)) 222))
      (is (contains? (set (map #(get % "id") range-docs)) 333333)))))