(ns nd-db.util-test
  (:require [nd-db.util :as sut]
            [clojure.test :refer :all]))

(deftest db?
  (testing "Somewhat valid check with db?"
    (is (sut/db? {:index {}
                :filename ""})))
  (testing "Unreal test with db?"
    (is (not (sut/db? {})))))
