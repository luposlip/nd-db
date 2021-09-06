(ns nd-db.util-test
  (:require [nd-db.util :as t]
            [clojure.test :refer :all]))

(deftest db?
  (testing "Somewhat valid check with db?"
    (is (t/db? (future {:index {}
                        :filename ""}))))
  (testing "Unreal test with db?"
    (is (not (t/db? (future {}))))))

(deftest rx-str->id+fn
  ;; TODO Implement!
  ;; also name-type-> and path->
  (is true))
