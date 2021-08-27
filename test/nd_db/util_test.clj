(ns nd-db.util-test
  (:require [nd-db.util :as t]
            [clojure.test :refer :all]))

(deftest db?
  (testing "Somewhat valid check with db?"
    (is (t/db? (future {:index {}
                        :filename ""}))))
  (testing "Unreal test with db?"
    (is (not (t/db? (future {}))))))

(deftest name-type->id+fn
  (testing "Generating map with :id-fn from json name"
    (let [{:keys [id-fn]} (t/name-type->id+fn {:id-name "id"
                                               :id-type :integer})]
      (is (fn? id-fn))
      (is (= 2 (id-fn "{\"id\":2}")))
      
      (testing "Implicit :id-type :string, explicit :source-type :integer"
        (is (= "2" ((:id-fn (t/name-type->id+fn {:id-name "id"
                                                 :source-type :integer}))
                    "{\"id\":2}"))))
      
      (testing "Explicit :id-type :integer, implicit :source-type :integer"
        (is (= 2 ((:id-fn (t/name-type->id+fn {:id-name "id"
                                               :id-type :integer}))
                  "{\"id\":2}"))))
      
      (testing "Explicit :id-type :string, implicit :source-type :string"
        (is (= "2" ((:id-fn (t/name-type->id+fn {:id-name "id"
                                                 :id-type :string}))
                    "{\"id\":\"2\"}"))))
      
      (testing "Explicit :id-type :string, explicit :source-type :string"
        (is (= "2" ((:id-fn (t/name-type->id+fn {:id-name "id"
                                                 :id-type :string
                                                 :source-type :string}))
                    "{\"id\":\"2\"}"))))

      (testing "Explicit :id-type :string, explicit :source-type :integer"
        (is (= "2" ((:id-fn (t/name-type->id+fn {:id-name "id"
                                                 :id-type :string
                                                 :source-type :integer}))
                    "{\"id\":2}"))))

      (testing "Explicit :id-type :integer, explicit :source-type :integer"
        (is (= 2 ((:id-fn (t/name-type->id+fn {:id-name "id"
                                               :id-type :integer
                                               :source-type :integer}))
                  "{\"id\":2}")))))))

(deftest rx-str->id+fn
  ;; TODO Implement!
  (is true))
