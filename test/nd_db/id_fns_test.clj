(ns nd-db.id-fns-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer :all]
            [nd-db
             [io :as ndio]
             [id-fns :as sut]]))

(deftest name-type->id+fn-1
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

(deftest name-type->id+fn-2
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
        nippy-str (ndio/->str data)
        edn-str (str data)
        res1 (#'sut/pathy->id+fn [:a :b :c] ndio/str->)
        res2 (#'sut/pathy->id+fn [:a :b :c] edn/read-string)
        res3 (#'sut/pathy->id+fn :d ndio/str->)
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
