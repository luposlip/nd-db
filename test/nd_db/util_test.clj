(ns nd-db.util-test
  (:require [nd-db.util :as t]
            [clojure.test :refer :all]))

(deftest db?
  (testing "Somewhat valid check with db?"
    (is (t/db? (future {:index {}
                        :filename ""}))))
  (testing "Unreal test with db?"
    (is (not (t/db? (future {}))))))
