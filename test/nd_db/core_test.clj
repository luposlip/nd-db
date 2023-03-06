(ns nd-db.core-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [nd-db
             [core :as sut]
             [util :as ndut]
             [io :as ndio]]))

(def by-id #(Integer. ^String (second (re-find #"^\{\"id\":(\d+)" %))))

(deftest query-single
  (testing ".ndjson file as random access database for single id"
    (is (= {:id 222
            :data 42}
           (sut/q (#'sut/raw-db
                   (ndio/parse-params {:id-fn by-id
                                       :filename "resources/test/test.ndjson"}))
                  222)))))

(deftest raw-db
  (testing "Getting a database"
    (is (ndut/db?
         (#'sut/raw-db (ndio/parse-params
                      {:id-fn by-id
                       :filename "resources/test/test.ndjson"})))))
  (testing "using :id-path as params"
    (is (= [{:id 333333
             :data {:datakey "datavalue"}}
            {:id 1
             :data ["some" "semi-random" "data"]}]
           (vec
            (sut/q (#'sut/raw-db (ndio/parse-params {:id-name "id"
                                                   :id-type :integer
                                                   :filename "resources/test/test.ndjson"}))
                   [333333 1 77]))))))

(deftest db
  (let [params {:id-fn by-id
                :filename "resources/test/test.ndjson"}]
    (try (io/delete-file (#'ndio/serialize-db-filename params)) (catch Throwable _ nil))
    (testing "Getting a database the first time (incl. serialization)"
      (is (ndut/db? (sut/db params))))
    (testing "Getting a database the second time (deserialization)"
      ;;(is (ndut/db? (sut/db params)))
      )))

(deftest query-raw-db
  (testing ".ndjson file as random access database for multiple ids"

    (testing "using :id-fn-key and :id-fn as params"
      (is (= [{:id 333333
               :data {:datakey "datavalue"}}
              {:id 1
               :data ["some" "semi-random" "data"]}]
             (vec
              (sut/q (#'sut/raw-db (ndio/parse-params {:id-fn by-id
                                                     :filename  "resources/test/test.ndjson"}))
                     [333333 1 77])))))

    (testing "using :id-name and :id-type as params"
      (is (= [{:id 333333
               :data {:datakey "datavalue"}}
              {:id 1
               :data ["some" "semi-random" "data"]}]
             (vec
              (sut/q (#'sut/raw-db (ndio/parse-params {:id-name "id"
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
              (sut/q (#'sut/raw-db (ndio/parse-params {:id-rx-str "^\\{\"id\":(\\d+)"
                                                     :filename  "resources/test/test.ndjson"}))
                     [333333 1 77])))))))

(deftest query-nippy-raw-db
  (testing ".ndnippy file as random access database"
    (testing "using :id-fn-key and :id-fn as params"
      (is (= [{:id 333333
               :data {:datakey "datavalue"}}
              {:id 1
               :data ["some" "semi-random" "data"]}]
             (vec
              (sut/q (#'sut/raw-db (ndio/parse-params :id-path [:id]
                                                    :filename "resources/test/test.ndnippy"))
                     [333333 1 77])))))))

(deftest query-edn-raw-db

  (testing "throw exception when using .ndedn plus :id-name/:id-type combo"
    (is (thrown? clojure.lang.ExceptionInfo
                 (#'sut/raw-db (ndio/parse-params {:id-name "id"
                                                 :id-type :integer
                                                 :filename "resources/test/test.ndedn"})))))

  (testing ".ndedn works with :id-rx-str"
    (is (= [{:id 312 :adresse "Adresse 3"}
            {:id 123 :adresse "Adresse 1"}]
           (vec (sut/q (#'sut/raw-db (ndio/parse-params {:id-rx-str "^\\{:id (\\d+)"
                                                       :filename "resources/test/test.ndedn"}))
                       [312 100 123]))))))


(deftest explicit-index-folder
  (let [folder (str (System/getProperty "java.io.tmpdir") "/explicit-folder")
        params {:id-fn by-id
                :filename "resources/test/test.ndjson"
                :index-folder folder}
        serialized-filename (#'ndio/serialize-db-filename params)]
    (io/make-parents (str folder "/null"))
    (try (io/delete-file serialized-filename) (catch Throwable _ nil))
    (is (not (.isFile (io/file serialized-filename))))
    (testing "Getting a database the first time (incl. serialization)"
      (is (ndut/db? (sut/db params))))
    (is (.isFile (io/file serialized-filename)))
    (testing "Getting a database the second time (deserialization)"
      (is (ndut/db? (sut/db params))))))

(deftest dont-index-persist
  (let [params {:id-fn by-id
                :filename "resources/test/test.ndjson"
                :index-persist? false}
        serialized-filename (#'ndio/serialize-db-filename params)]
    (try (io/delete-file serialized-filename) (catch Throwable _ nil))
    (is (not (.isFile (io/file serialized-filename))))
    (testing "Getting a database the first time (incl. serialization)"
      (is (ndut/db? (sut/db params))))
    (testing "Index is not persisted"
      (is (not (.isFile (io/file serialized-filename)))))))

(deftest lazy-docs
  (let [db (#'sut/raw-db
            (ndio/parse-params :id-path [:id]
                               :filename "resources/test/test.ndnippy"))
        docs (sut/lazy-docs db)]

    (is (= clojure.lang.LazySeq (type docs)))
    (is (map? (first docs)))
    (is (= 3 (count docs)))))
