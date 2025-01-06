(ns nd-db.index-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer :all]
            [nd-db
             [core :as nddb]
             [index :as sut]]
            [nd-db.util :as ndut])
  (:import clojure.lang.ExceptionInfo))

(def by-id #(Integer. ^String (second (re-find #"^\{\"id\":(\d+)" %))))

(deftest index-id
  (testing "Index ID"
    (is (= [1 222 333333]
           (sut/index-id :filename "resources/test/test.ndjson"
                         :id-fn by-id)))))

(deftest index-id-edn
  (testing "Index ID, EDN edition"
    (is (= [123 231 312]
           (sut/index-id {:filename "resources/test/test.ndedn"
                          :id-fn #(:id (edn/read-string %))
                          :doc-type :edn})))))

(deftest create-index
  (let [{:keys [filename id-fn]}
        {:filename "resources/test/test.ndjson"
         :id-fn by-id}]
    (testing "Database index"
      (is (= {1 [0 49]
              222 [50 22]
              333333 [73 46]}
             (sut/create-index filename id-fn))))))

(deftest reader
  (let [db (nddb/db :filename "resources/test/test.ndnippy"
                    :id-path :id)]
    (with-open [r (sut/reader db)]
      (is (= 3 (count (line-seq r)))))))

(deftest append
  (let [{:keys [doc-emitter] :as db} (nddb/db :filename "resources/test/test.csv"
                                              :col-separator ","
                                              :id-path :a)
        _ (-> db :index deref) ;; pre-heat: make sure to realize the index
                               ;; before adding the next entry
        new-id (rand-int 999999999)
        doc {:a new-id :b 8888 :c 7777}
        doc-emission-str (doc-emitter doc)
        new-db (sut/append db doc doc-emission-str)
        new-index-keys (-> new-db :index deref keys set)]
    (is (ndut/db? new-db))
    (is (= new-id (new-index-keys new-id)))))

(deftest empty-db
  (is (thrown?
       ExceptionInfo
       (nddb/db :filename "resources/test/empty.ndnippy"
                :id-path :id))))
