(ns ndjson-db.core-test
  (:require [clojure.test :refer :all]
            [ndjson-db.core :as db]))

(def by-id #(Integer. ^String (second (re-find #"^\{\"id\":(\d+)" %))))

(deftest index-id
  (testing "Index ID"
    (is (= [1 222 333333]
           (db/index-id {:filename "resources/test/test.ndjson"
                         :id-fn by-id})))))

(deftest index-id-edn
  (testing "Index ID, EDN edition"
    (is (= [123 231 312]
           (db/index-id {:filename "resources/test/test.ndedn"
                         :id-fn #(:id (clojure.edn/read-string %))
                         :doc-type :edn})))))

(deftest index*
  (let [{:keys [filename id-fn] :as params}
        {:filename "resources/test/test.ndjson"
         :id-fn by-id}
        idx-id (db/index-id params)]
    (swap! db/index-fns ;; Test hack
           assoc-in
           [filename idx-id]
           id-fn)
    (testing "Database index" 
      (is (= {1 [0 49]
              222 [50 22]
              333333 [73 46]}
             (db/index* filename idx-id))))))

(deftest db?
  (testing "Somewhat valid check with db?"
    (is (db/db? (future {:index {}
                         :filename ""}))))
  (testing "Unreal test with db?"
    (is (not (db/db? (future {}))))))

(deftest db
  (testing "Getting a database"
    (is (db/db?
         (db/db {:id-fn by-id
                 :filename "resources/test/test.ndjson"})))))

(deftest query-single
  (testing ".ndjson file as random access database for single id" 
    (is (= {:id 222
            :data 42}
           (db/q (db/db {:id-fn by-id
                         :filename "resources/test/test.ndjson"})
                 222)))))

(deftest get-id-fn
  (testing "Generating map with :id-fn from json name"
    (let [id-fn (db/get-id-fn {:id-name "id"
                               :id-type :integer})]
      (is (fn? id-fn))
      (is (= 2 (id-fn "{\"id\":2}")))
      
      (testing "Implicit :id-type :string, explicit :source-type :integer"
        (is (= "2" ((db/get-id-fn {:id-name "id"
                                   :source-type :integer})
                    "{\"id\":2}"))))
      
      (testing "Explicit :id-type :integer, implicit :source-type :integer"
        (is (= 2 ((db/get-id-fn {:id-name "id"
                                 :id-type :integer})
                  "{\"id\":2}"))))
      
      (testing "Explicit :id-type :string, implicit :source-type :string"
        (is (= "2" ((db/get-id-fn {:id-name "id"
                                   :id-type :string})
                    "{\"id\":\"2\"}"))))
      
      (testing "Explicit :id-type :string, explicit :source-type :string"
        (is (= "2" ((db/get-id-fn {:id-name "id"
                                   :id-type :string
                                   :source-type :string})
                    "{\"id\":\"2\"}"))))

      (testing "Explicit :id-type :string, explicit :source-type :integer"
        (is (= "2" ((db/get-id-fn {:id-name "id"
                                   :id-type :string
                                   :source-type :integer})
                    "{\"id\":2}"))))

      (testing "Explicit :id-type :integer, explicit :source-type :integer"
        (is (= 2 ((db/get-id-fn {:id-name "id"
                                 :id-type :integer
                                 :source-type :integer})
                  "{\"id\":2}")))))))

(deftest query
  (testing ".ndjson file as random access database for multiple ids"

    (testing "using :id-fn-key and :id-fn as params"
      (is (= [{:id 333333
               :data {:datakey "datavalue"}}
              {:id 1
               :data ["some" "semi-random" "data"]}]
             (into []
                   (db/q (db/db {:id-fn by-id
                                 :filename  "resources/test/test.ndjson"})
                         [333333 1 77])))))
    
    (testing "using :id-name and :id-type as params"
      (is (= [{:id 333333
               :data {:datakey "datavalue"}}
              {:id 1
               :data ["some" "semi-random" "data"]}]
             (into []
                   (db/q (db/db {:id-name "id"
                                 :id-type :string
                                 :source-type :integer
                                 :filename  "resources/test/test.ndjson"})
                         ["333333" "1" "77"])))))))

