(ns nd-db.io-test
  (:require [clojure
             [test :refer :all]
             [string :as s]
             [edn :as edn]]
            [nd-db.io :as sut]))

(deftest name-type->id+fn
  (testing "Generating map with :id-fn from json name"
    (let [{:keys [id-fn]} (#'sut/name-type->id+fn {:id-name "id"
                                                 :id-type :integer})]
      (is (fn? id-fn))
      (is (= 2 (id-fn "{\"id\":2}")))

      (testing "Implicit :id-type :string, explicit :source-type :integer"
        (is (= "2" ((:id-fn (#'sut/name-type->id+fn {:id-name     "id"
                                                   :source-type :integer}))
                    "{\"id\":2}"))))

      (testing "Explicit :id-type :integer, implicit :source-type :integer"
        (is (= 2 ((:id-fn (#'sut/name-type->id+fn {:id-name "id"
                                                 :id-type :integer}))
                  "{\"id\":2}"))))

      (testing "Explicit :id-type :string, implicit :source-type :string"
        (is (= "2" ((:id-fn (#'sut/name-type->id+fn {:id-name "id"
                                                   :id-type :string}))
                    "{\"id\":\"2\"}"))))

      (testing "Explicit :id-type :string, explicit :source-type :string"
        (is (= "2" ((:id-fn (#'sut/name-type->id+fn {:id-name     "id"
                                                   :id-type     :string
                                                   :source-type :string}))
                    "{\"id\":\"2\"}"))))

      (testing "Explicit :id-type :string, explicit :source-type :integer"
        (is (= "2" ((:id-fn (#'sut/name-type->id+fn {:id-name     "id"
                                                   :id-type     :string
                                                   :source-type :integer}))
                    "{\"id\":2}"))))

      (testing "Explicit :id-type :integer, explicit :source-type :integer"
        (is (= 2 ((:id-fn (#'sut/name-type->id+fn {:id-name     "id"
                                                 :id-type     :integer
                                                 :source-type :integer}))
                  "{\"id\":2}")))))))

(deftest parse-params
  (testing "FAIL id-path with EDN input"
    (is (thrown? clojure.lang.ExceptionInfo (sut/parse-params :filename "test.ndedn"
                                                              :id-path [:id]))))
  (testing "FAIL id-path with JSON input"
    (is (thrown? clojure.lang.ExceptionInfo (sut/parse-params {:filename "test.ndjson"
                                                               :id-path [:id]}))))
  (testing "SUCCEED id-path with nippy input"
    (let [{:keys [id-fn]} (sut/parse-params {:filename "test.ndnippy"
                                             :id-path [:id]})]
      (is (= 123 (id-fn (sut/->str {:id 123})))))))

(deftest serialized-db-filepath
  (let [input-folder "resources"
        params {:filename (str input-folder "/test/test.ndnippy")}]
    (testing "Default is to generate the index in the same folder as the db"
        (is (s/starts-with?
             (sut/serialized-db-filepath params)
             input-folder)))
    (testing "Generate index in a specific folder"
        (is (not
             (s/starts-with?
              (sut/serialized-db-filepath (assoc params
                                                 :index-folder input-folder))
              (str input-folder "/test/")))))))

(deftest str->str
  (let [data {:a "asdf1234!"
              :b [:c "d" 'e]}]
    (is (= data (-> data sut/->str sut/str->)))))

(deftest maybe-update-filename
  (is (= {:filename "a"}
         (#'sut/maybe-update-filename {:filename "a"} "a")))
  (is (= {:filename "b"
          :org-filename "a"}
         (#'sut/maybe-update-filename {:filename "a"} "b"))))

(deftest name-type->id+fn
  (let [res (#'sut/name-type->id+fn {:id-name "id"
                                     :id-type :integer})]
    (is (fn? (:id-fn res)))
    (is (string? (:idx-id res))))
  (let [res (#'sut/name-type->id+fn {:id-name "id"
                                     :id-type :integer
                                     :source-type :string})]
    (is (fn? (:id-fn res)))
    (is (string? (:idx-id res)))))

(deftest pathy->id+fn
  (let [data {:a {:b {:c 1}}
              :d 2}
        nippy-str (sut/->str data)
        edn-str (str data)
        res1 (#'sut/pathy->id+fn [:a :b :c])
        res2 (#'sut/pathy->id+fn [:a :b :c] edn/read-string)
        res3 (#'sut/pathy->id+fn :d)
        res4 (#'sut/pathy->id+fn :d edn/read-string)]

    (testing ":idx-id is correctly calculated"
      (is (= "abc" (:idx-id res1)))
      (is (= "abc" (:idx-id res2)))
      (is (= "d" (:idx-id res3)))
      (is (= "d" (:idx-id res4))))

    (testing ":id-fn works as expected"
      (is (fn? (:id-fn res1)))
      (is (= 1 ((:id-fn res1) nippy-str)))
      (is (= 1 ((:id-fn res2) edn-str)))
      (is (= 2 ((:id-fn res3) nippy-str)))
      (is (= 2 ((:id-fn res4) edn-str))))))

(deftest infer-doctype
  (is (= :csv (#'sut/infer-doctype "/some/path/to/a.csv")))
  (is (= :nippy (#'sut/infer-doctype "/some/path/to/a.ndnippy")))
  (is (= :unknown (#'sut/infer-doctype "/some/path/to/a.zip"))))
