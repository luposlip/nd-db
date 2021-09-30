(ns nd-db.io-test
  (:require [nd-db.io :as sut]
            [clojure.test :refer :all]))

(deftest name-type->id+fn
  (testing "Generating map with :id-fn from json name"
    (let [{:keys [id-fn]} (sut/name-type->id+fn {:id-name "id"
                                                 :id-type :integer})]
      (is (fn? id-fn))
      (is (= 2 (id-fn "{\"id\":2}")))
      
      (testing "Implicit :id-type :string, explicit :source-type :integer"
        (is (= "2" ((:id-fn (sut/name-type->id+fn {:id-name "id"
                                                   :source-type :integer}))
                    "{\"id\":2}"))))
      
      (testing "Explicit :id-type :integer, implicit :source-type :integer"
        (is (= 2 ((:id-fn (sut/name-type->id+fn {:id-name "id"
                                                 :id-type :integer}))
                  "{\"id\":2}"))))
      
      (testing "Explicit :id-type :string, implicit :source-type :string"
        (is (= "2" ((:id-fn (sut/name-type->id+fn {:id-name "id"
                                                   :id-type :string}))
                    "{\"id\":\"2\"}"))))
      
      (testing "Explicit :id-type :string, explicit :source-type :string"
        (is (= "2" ((:id-fn (sut/name-type->id+fn {:id-name "id"
                                                   :id-type :string
                                                   :source-type :string}))
                    "{\"id\":\"2\"}"))))

      (testing "Explicit :id-type :string, explicit :source-type :integer"
        (is (= "2" ((:id-fn (sut/name-type->id+fn {:id-name "id"
                                                   :id-type :string
                                                   :source-type :integer}))
                    "{\"id\":2}"))))

      (testing "Explicit :id-type :integer, explicit :source-type :integer"
        (is (= 2 ((:id-fn (sut/name-type->id+fn {:id-name "id"
                                                 :id-type :integer
                                                 :source-type :integer}))
                  "{\"id\":2}")))))))

(deftest parse-params
  (testing "FAIL id-path with EDN input"
    (is (thrown? clojure.lang.ExceptionInfo (sut/parse-params {:filename "test.ndedn"
                                                               :id-path [:id]}))))
  (testing "FAIL id-path with JSON input"
    (is (thrown? clojure.lang.ExceptionInfo (sut/parse-params {:filename "test.ndjson"
                                                               :id-path [:id]}))))
  (testing "SUCCEED id-path with nippy input"
    (let [{:keys [id-fn]} (sut/parse-params {:filename "test.ndnippy"
                                             :id-path [:id]})]
      (is (= 123 (id-fn (sut/->str {:id 123})))))))
