(ns nd-db.convert-test
  (:require [nd-db
             [core :as nddb]
             [io :as ndio]
             [util :as ndut]
             [convert :as sut]]
            [clojure.test :refer :all]))

(def by-id #(Integer. ^String (second (re-find #"^\{\"id\":(\d+)" %))))

(deftest db->ndnippy
  (let [db (nddb/db {:filename "resources/test/test.ndjson"
                     :id-fn by-id})
        new-filename "/tmp/test.ndnippy"
        doc-count (sut/db->ndnippy db new-filename)]
    (testing "3 documents have been converted"
      (is (= 3 doc-count)))
    (testing "The generated ndnippy database can be used"
      (let [new-db (#'nddb/raw-db (ndio/parse-params :filename new-filename
                                                   :id-path [:id]))]
        (is (ndut/db? new-db))
        (is (= doc-count (-> new-db :index deref keys count)))
        (is (=  #{1 222 333333} (-> new-db :index deref keys set)))))))

(deftest db->ndnippy-db
  (let [db (nddb/db {:filename "resources/test/test.ndjson"
                     :id-fn by-id})
        new-filename "/tmp/test.ndnippy"
        doc-count (sut/db->ndnippy db new-filename)]
    (testing "3 documents have been converted"
      (is (= 3 doc-count)))
    (testing "The generated ndnippy database can be used"
      (let [new-db (#'nddb/raw-db (ndio/parse-params {:filename new-filename
                                                    :id-path [:id]}))]
        (is (ndut/db? new-db))
        (is (= doc-count (-> new-db :index deref keys count)))
        (is (=  #{1 222 333333} (-> new-db :index deref keys set)))))))
