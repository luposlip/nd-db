(ns nd-db.io-test
  (:require [clojure
             [test :refer :all]
             [string :as s]]
            [taoensso.nippy :as nippy]
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
  (is (= :csv (#'sut/infer-doctype {:filename "/some/path/to/a.csv"})))
  (is (= :nippy (#'sut/infer-doctype {:filename "/some/path/to/a.ndnippy"})))
  (is (= :json (#'sut/infer-doctype {:filename "/some/path/to/a.ndjson"})))
  (is (= :edn (#'sut/infer-doctype {:filename "/some/path/to/a.ndedn"}))))

(deftest infer-zip-doctype
  (is (= :zip/unknown (#'sut/infer-doctype {:filename "unknown.zip"})))
  (is (= :zip/unknown (#'sut/infer-doctype {:filename "argh.zip" :doc-type :argh})))
  (is (= :zip/json (#'sut/infer-doctype {:filename "jsons.zip" :doc-type :json})))
  (is (= :zip/edn (#'sut/infer-doctype {:filename "edns.zip" :doc-type :edn}))))

(deftest last-line
  (is (= "c,5,6" (sut/last-line "resources/test/test.csv"))))

(deftest params->doc-parser
  (is (= :hello ((sut/params->doc-parser {:doc-type :ged
                                          :doc-parser (constantly :hello)})
                 "{:input {:data \"hey!\"}"))
      "Test that explicit :doc-parser ignores set :doc-type because the :doc-type has no namespace")
  (is (= "hey!" (get-in
                 ((sut/params->doc-parser {:doc-type :edn})
                  (.getBytes "{:input {:data \"hey!\"}}"))
                 [:input :data]))
      "Simply parses as EDN")
  (is (= "hey!" (get-in
                 ((sut/params->doc-parser {:doc-type :json})
                  (.getBytes "{\"input\": {\"data\": \"hey!\"}}"))
                 [:input :data]))
      "Simply parses as JSON"))

(deftest params->doc-parser-zippy-edition
  (is (= "zippy here!" (get-in
                 ((sut/params->doc-parser {:doc-type :zippy})
                  (nippy/freeze {:input {:data "zippy here!"}}))
                 [:input :data]))
      "Simply parses as nippy - for usage within a zip file"))
