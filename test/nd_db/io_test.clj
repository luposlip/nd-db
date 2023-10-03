(ns nd-db.io-test
  (:require [clojure
             [test :refer :all]
             [string :as s]]
            [nd-db.io :as sut]))

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

(deftest infer-doctype
  (is (= :csv (#'sut/infer-doctype "/some/path/to/a.csv")))
  (is (= :nippy (#'sut/infer-doctype "/some/path/to/a.ndnippy")))
  (is (= :unknown (#'sut/infer-doctype "/some/path/to/a.zip"))))

(deftest last-line
  (is (= "c,5,6" (sut/last-line "resources/test/test.csv"))))
