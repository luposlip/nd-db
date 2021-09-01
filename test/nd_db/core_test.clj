(ns nd-db.core-test
  (:require [clojure.test :refer :all]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [nd-db
             [core :as t]
             [util :as u]
             [io :as ndio]]))

(def by-id #(Integer. ^String (second (re-find #"^\{\"id\":(\d+)" %))))

(deftest index-id
  (testing "Index ID"
    (is (= [1 222 333333]
           (t/index-id {:filename "resources/test/test.ndjson"
                         :id-fn by-id})))))

(deftest index-id-edn
  (testing "Index ID, EDN edition"
    (is (= [123 231 312]
           (t/index-id {:filename "resources/test/test.ndedn"
                         :id-fn #(:id (edn/read-string %))
                         :doc-type :edn})))))

(deftest create-index
  (let [{:keys [filename id-fn] :as params}
        {:filename "resources/test/test.ndjson"
         :id-fn by-id}
        idx-id (t/index-id params)]
    (swap! t/index-fns ;; Test hack
           assoc-in
           [filename idx-id]
           id-fn)
    (testing "Database index" 
      (is (= {1 [0 49]
              222 [50 22]
              333333 [73 46]}
             (t/create-index filename idx-id))))))

(deftest query-single
  (testing ".ndjson file as random access database for single id" 
    (is (= {:id 222
            :data 42}
           (t/q (t/raw-db {:id-fn by-id
                           :filename "resources/test/test.ndjson"})
                222)))))

(deftest raw-db
  (testing "Getting a database"
    (is (u/db?
         (t/raw-db {:id-fn by-id
                    :filename "resources/test/test.ndjson"})))))

(deftest db
  (let [params {:id-fn by-id
                :filename "resources/test/test.ndjson"}]
    (try (io/delete-file (ndio/serialize-db-filename params)) (catch Throwable _ nil))
    (testing "Getting a database the first time (incl. serialization)"
      (is (u/db? (t/db params))))
    (testing "Getting a database the second time (deserialization)"
      (is (u/db? (t/db params))))))

(deftest query-raw-db
  (testing ".ndjson file as random access database for multiple ids"

    (testing "using :id-fn-key and :id-fn as params"
      (is (= [{:id 333333
               :data {:datakey "datavalue"}}
              {:id 1
               :data ["some" "semi-random" "data"]}]
             (vec
              (t/q (t/raw-db {:id-fn by-id
                              :filename  "resources/test/test.ndjson"})
                   [333333 1 77])))))
    
    (testing "using :id-name and :id-type as params"
      (is (= [{:id 333333
               :data {:datakey "datavalue"}}
              {:id 1
               :data ["some" "semi-random" "data"]}]
             (vec
              (t/q (t/raw-db (t/parse-params {:id-name "id"
                                              :id-type :string
                                              :source-type :integer
                                              :filename  "resources/test/test.ndjson"}))
                   ["333333" "1" "77"])))))

    (testing "using :id-rx-str as param"
      (is (= [{:id 333333
               :data {:datakey "datavalue"}}
              {:id 1
               :data ["some" "semi-random" "data"]}]
             (vec
              (t/q (t/raw-db (t/parse-params {:id-rx-str "^\\{\"id\":(\\d+)"
                                              :filename  "resources/test/test.ndjson"}))
                   [333333 1 77])))))))


(deftest explicit-index-folder
  (let [folder (str (System/getProperty "java.io.tmpdir") "/explicit-folder")
        params {:id-fn by-id
                :filename "resources/test/test.ndjson"
                :index-folder folder}
        serialized-filename (ndio/serialize-db-filename params)]
    (io/make-parents (str folder "/null"))
    (try (io/delete-file serialized-filename) (catch Throwable _ nil))
    (is (not (.isFile (io/file serialized-filename))))
    (testing "Getting a database the first time (incl. serialization)"
      (is (u/db? (t/db params))))
    (is (.isFile (io/file serialized-filename)))
    (testing "Getting a database the second time (deserialization)"
      (is (u/db? (t/db params))))))

(deftest dont-index-persist
  (let [params {:id-fn by-id
                :filename "resources/test/test.ndjson"
                :index-persist? false}
        serialized-filename (ndio/serialize-db-filename params)]
    (try (io/delete-file serialized-filename) (catch Throwable _ nil))
    (is (not (.isFile (io/file serialized-filename))))
    (testing "Getting a database the first time (incl. serialization)"
      (is (u/db? (t/db params))))
    (testing "Index is not persisted"
      (is (not (.isFile (io/file serialized-filename)))))))
