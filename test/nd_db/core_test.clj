(ns nd-db.core-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [nd-db
             [core :as sut]
             [util :as ndut]
             [io :as ndio]
             [index :as ndix]]
            [nd-db.core :as nddb]))

(def by-id #(Integer. ^String (second (re-find #"^\{\"id\":(\d+)" %))))

(defn delete-meta [db]
  (io/delete-file (ndio/serialized-db-filepath db)))

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
    (try (io/delete-file (#'ndio/serialized-db-filepath params)) (catch Throwable _ nil))
    (testing "Getting a database the first time (incl. serialization)"
      (is (ndut/db? (sut/db params))))
    (testing "Getting a database the second time (deserialization)"
      (is (ndut/db? (sut/db params))))))

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
        serialized-filename (#'ndio/serialized-db-filepath params)]
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
        serialized-filename (#'ndio/serialized-db-filepath params)]
    (try (io/delete-file serialized-filename) (catch Throwable _ nil))
    (is (not (.isFile (io/file serialized-filename))))
    (testing "Getting a database the first time (incl. serialization)"
      (is (ndut/db? (sut/db params))))
    (testing "Index is not persisted"
      (is (not (.isFile (io/file serialized-filename)))))))

(deftest lazy-docs
  (let [db (#'sut/raw-db
            (ndio/parse-params :id-path :id
                               :filename "resources/test/test.ndnippy"))
        docs (sut/lazy-docs db)]

    (is (= clojure.lang.LazySeq (type docs)))
    (is (map? (first docs)))
    (is (= 3 (count docs)))))

(deftest lazy-ids
  (let [db (sut/db
            (ndio/parse-params :id-path :id
                               :filename "resources/test/test.ndnippy"))]
    (with-open [r (ndix/reader db)]
      (let [ids (sut/lazy-ids r)]
        (is (= clojure.lang.LazySeq (type ids)))
        (is (= 3 (count ids)))))
    (delete-meta db)))

(deftest query-csv
  (let [db (sut/db :filename "resources/test/test.csv"
                   :col-separator ","
                   :id-path :a)]
    (is (= {:a "c", :b 5, :c 6} (sut/q db "c")))
    (is (= {:a 3, :b "b", :c 4} (sut/q db 3)))
    (delete-meta db)))

(deftest emit-doc
  (let [tmp-filename "resources/test/tmp-test.csv"
        inf (io/file "resources/test/test.csv")
        outf (io/file tmp-filename)
        _ (io/copy inf outf)
        {:keys [doc-emitter] :as db} (nddb/db {:filename tmp-filename
                                               :col-separator ","
                                               :id-path :a})
        old-count (-> db :index deref count)
        new-id (rand-int 99999999)
        doc {:q "q" :a new-id :b "b movie" :c "sharp"}
        doc-emission-str (doc-emitter doc)]
    (#'sut/emit-doc db doc-emission-str)
    (is (= (ndio/last-line tmp-filename) doc-emission-str))
    (is (= (+ 3 old-count) (-> outf io/reader line-seq count))
        "1 line for header, 1 revision of id=1, 1 new doc")
    (delete-meta db)
    (io/delete-file tmp-filename)))

(deftest append
  (let [tmp-filename "resources/test/tmp-test.csv"
        inf (io/file "resources/test/test.csv")
        outf (io/file tmp-filename)
        _ (io/copy inf outf)
        {:keys [doc-emitter] :as db} (nddb/db {:filename tmp-filename
                                               :col-separator ","
                                               :id-path :a})
        old-count (-> db :index deref count)
        new-id (rand-int 99999999)
        doc {:q "q" :a new-id :b "b movie" :c "sharp"}
        doc-emission-str (doc-emitter doc)
        new-db (sut/append db doc)]
    (is (= doc-emission-str (ndio/last-line tmp-filename))
        "Ensure that the last line of the database is now the new doc")
    (is (= (inc old-count) (-> new-db :index deref count))
        "Ensure that the new line is inserted into the database")
    (is (= (select-keys doc [:a :b :c]) (nddb/q new-db new-id))
        "Query new doc from new database index")
    (delete-meta new-db)
    (let [db (nddb/db {:filename tmp-filename
                       :col-separator ","
                       :id-path :a})]
      (with-open [r (ndix/reader db)]
        (is (= #{{:a 1 :b 7 :c "a"}
                 {:a 3 :b "b" :c 4}
                 {:a "c" :b 5 :c 6}
                 {:a new-id :b "b movie" :c "sharp"}}
               (set (nddb/q db (nddb/lazy-ids db))))
            "Check that all docs, old and new, can be correctly read from db"))
      (delete-meta db))
    (io/delete-file "resources/test/tmp-test.csv")))

(deftest append-new-versions
  (let [tmp-filename "resources/test/tmp-test.csv"
        inf (io/file "resources/test/test.csv")
        outf (io/file tmp-filename)
        _ (io/copy inf outf)
        db (nddb/db {:filename tmp-filename
                     :col-separator ","
                     :id-path :a})
        doc {:a 1 :b "movie" :c "sharp"}
        new-db (sut/append db doc)]
    (-> new-db :index deref)
    (is (= {:a 1 :b 7 :c "a"} (nddb/q db 1)) "Old db returns old doc")
    (is (= doc (nddb/q new-db 1)) "New db returns new version")
    (delete-meta db)
    (io/delete-file "resources/test/tmp-test.csv")))

(deftest append-new-version-of-last-doc
  (let [tmp-filename "resources/test/tmp-test.csv"
        inf (io/file "resources/test/test.csv")
        outf (io/file tmp-filename)
        _ (io/copy inf outf)
        db (nddb/db {:filename tmp-filename
                     :col-separator ","
                     :id-path :a})
        doc {:a "c" :b 8 :c 9}
        new-db (sut/append db doc)]
    (-> new-db :index deref)
    (is (= {:a "c" :b 5 :c 6} (nddb/q db "c")) "Old db returns old doc")
    (is (= doc (nddb/q new-db "c")) "New db returns new version")
    (delete-meta db)
    (io/delete-file "resources/test/tmp-test.csv")))

(deftest query-historical-db
  (let [tmp-filename "resources/test/tmp-test.csv"
        inf (io/file "resources/test/test.csv")
        outf (io/file tmp-filename)
        _ (io/copy inf outf)
        db (nddb/db {:filename tmp-filename
                     :col-separator ","
                     :id-path :a})
        doc {:a "c" :b 8 :c 9}
        new-db (sut/append db doc)]
    (-> new-db :index deref)
    (is (= {:a "c" :b 5 :c 6} (nddb/q db "c")) "Old db returns old doc")
    (is (= doc (nddb/q new-db "c")) "New db returns new version")
    (delete-meta db)
    (io/delete-file "resources/test/tmp-test.csv"))
  )
