(ns nd-db.util-test
  (:require [nd-db.util :as sut]
            [clojure.test :refer :all]))

(deftest db?
  (testing "Somewhat valid check with db?"
    (is (sut/db? {:index {}
                :filename ""})))
  (testing "Unreal test with db?"
    (is (not (sut/db? {})))))

(deftest index-of
  (is (= 0 (sut/index-of :a [:a :b :c])))
  (is (= 1 (sut/index-of :b [:a :b :c])))
  (is (= 2 (sut/index-of :c [:a :b :c])))
  (is (nil? (sut/index-of :e [:a :b :c])))
  (is (= 1 (sut/index-of [:c :d] {:a :b :c :d}))))
