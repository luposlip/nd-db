(ns nd-db.core-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [nd-db
             [core :as sut]
             [util :as ndut]
             [io :as ndio]
             [index :as ndix]]))

(def by-id #(Integer. ^String (second (re-find #"^\{\"id\":(\d+)" %))))

(defn delete-meta [db]
  (io/delete-file (ndio/serialized-db-filepath db)))

(defn db-index-line-count [db]
  (with-open [r (io/reader (ndio/serialized-db-filepath db))]
    (-> r line-seq count)))

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
        {:keys [doc-emitter] :as db} (sut/db {:filename tmp-filename
                                               :col-separator ","
                                               :id-path :a})
        old-count (-> db :index deref count)
        new-id (rand-int 99999999)
        doc {:q "q" :a new-id :b "b movie" :c "sharp"}
        doc-emission-str (doc-emitter doc)]
    (#'sut/emit-docs db doc-emission-str)
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
        {:keys [doc-emitter] :as db} (sut/db {:filename tmp-filename
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
    (is (= (select-keys doc [:a :b :c]) (sut/q new-db new-id))
        "Query new doc from new database index")
    (delete-meta new-db)
    (let [db (sut/db {:filename tmp-filename
                       :col-separator ","
                       :id-path :a})]
      (with-open [r (ndix/reader db)]
        (is (= #{{:a 1 :b 7 :c "a"}
                 {:a 3 :b "b" :c 4}
                 {:a "c" :b 5 :c 6}
                 {:a new-id :b "b movie" :c "sharp"}}
               (set (sut/q db (sut/lazy-ids db))))
            "Check that all docs, old and new, can be correctly read from db"))
      (delete-meta db))
    (io/delete-file tmp-filename)))

(deftest append-new-versions
  (let [tmp-filename "resources/test/tmp-test.csv"
        inf (io/file "resources/test/test.csv")
        outf (io/file tmp-filename)
        _ (io/copy inf outf)
        db (sut/db {:filename tmp-filename
                     :col-separator ","
                     :id-path :a})
        doc {:a 1 :b "movie" :c "sharp"}
        new-db (sut/append db doc)]
    (-> new-db :index deref)
    (is (= {:a 1 :b 7 :c "a"} (sut/q db 1)) "Old db returns old doc")
    (is (= doc (sut/q new-db 1)) "New db returns new version")
    (delete-meta db)
    (io/delete-file tmp-filename)))

(deftest append-new-version-of-last-doc
  (let [tmp-filename "resources/test/tmp-test.csv"
        inf (io/file "resources/test/test.csv")
        outf (io/file tmp-filename)
        _ (io/copy inf outf)
        db (sut/db {:filename tmp-filename
                     :col-separator ","
                     :id-path :a})
        doc {:a "c" :b 8 :c 9}
        new-db (sut/append db doc)]
    (-> new-db :index deref)
    (is (= {:a "c" :b 5 :c 6} (sut/q db "c")) "Old db returns old doc")
    (is (= doc (sut/q new-db "c")) "New db returns new version")
    (delete-meta db)
    (io/delete-file tmp-filename)))

(deftest append-new-versions
  (let [tmp-filename "resources/test/tmp-test.csv"
        inf (io/file "resources/test/test.csv")
        outf (io/file tmp-filename)
        _ (io/copy inf outf)
        db (sut/db {:filename tmp-filename
                     :col-separator ","
                     :id-path :a})
        newest-doc {:a "c" :b 12 :c 13}
        docs [{:a "c" :b 8 :c 9} {:a "c" :b 10 :c 11} newest-doc]
        new-db (sut/append db docs)]
    (-> db :index deref)
    (-> new-db :index deref)
    (is (= {:a "c" :b 5 :c 6} (sut/q db "c")) "Old db returns old doc")
    (is (= newest-doc (sut/q new-db "c")) "New db returns newest version")
    (delete-meta db)
    (io/delete-file tmp-filename)))

(deftest append-to-nippy
  (let [tmp-filename "resources/test/tmp-test.ndnippy"
        inf (io/file "resources/test/test.ndnippy")
        outf (io/file tmp-filename)
        _ (io/copy inf outf)
        db (sut/db :filename tmp-filename
                    :id-path :id)
        _ (-> db :index deref)
        pre-idx-lines (db-index-line-count db)
        docs [{:id 1 :b "c" :d "e"} {:id 123 :data [:x {:y "z"}] :q :p} {:id 222 :new "data"}]
        new-db (sut/append db docs)]
    (-> new-db :index deref)
    (is (= (+ pre-idx-lines (count docs)) (db-index-line-count new-db))
        "New index line count is old line count plus docs appended")
    (is (not= (first docs) (sut/q db 1)) "Old db returns old doc")
    (is (= (first docs) (sut/q new-db 1)) "New db returns new version")

    (let [fresh-db (sut/db :filename tmp-filename :id-path :id)]
      (-> fresh-db :index deref)
      (is (= (peek docs) (-> fresh-db (sut/q 222))) "Re-read updated index & q"))
    (delete-meta db)
    (io/delete-file tmp-filename)))

(deftest or-q
  (let [csv-db (sut/db :filename "resources/test/test.csv"
                       :col-separator ","
                       :id-path :a)
        json-db (sut/db :filename "resources/test/test.ndjson"
                        :id-fn by-id)]
    (is (= {:a 1 :b 7 :c "a"}
           (sut/or-q [csv-db json-db] 1))
        "ID exist in both databases, return from the first (CSV)")
    (is (= {:id 1 :data ["some" "semi-random" "data"]}
           (sut/or-q [json-db csv-db] 1))
        "ID exist in both databases, return from the first (JSON)")
    (is (= {:id 222 :data 42}
           (sut/or-q [csv-db json-db] 222))
        "ID only exist in the second database (JSON)")
    (is (= {:a "c" :b 5 :c 6}
           (sut/or-q [json-db csv-db] "c"))
        "ID only exist in the second database (CSV)")
    (is (= {:a "c" :b 5 :c 6}
           (sut/or-q [csv-db json-db] "c"))
        "ID only exist in the first database (CSV)")))

(deftest query-historical-db
  (let [tmp-filename "resources/test/tmp-test.csv"
        inf (io/file "resources/test/test.csv")
        outf (io/file tmp-filename)
        _ (io/copy inf outf)
        old-db (sut/db {:filename tmp-filename
                         :col-separator ","
                         :id-path :a
                         :log-limit 2})
        _ (-> old-db :index deref)
        current-db (sut/db {:filename tmp-filename
                             :col-separator ","
                             :id-path :a})
        _ (-> current-db :index deref)
        current-doc {:a "c" :b 5 :c 6}
        new-doc {:a "c" :b 8 :c 9}
        new-db (sut/append current-db new-doc)]

    (is (thrown? clojure.lang.ExceptionInfo (sut/append old-db new-doc))
        "Can't append to historical db version")

    (testing "Old database"
      ;; The test below won't work until historical versions have been
      ;; properly implemented! Right now index is persisted based on final map
      ;; of ids to offset/length pairs.. It should be written per document! So
      ;; that's a somewhat big refactoring..
      ;; TODO: Enable: (is (= 2 (-> old-db (sut/q 1) :b)) "This is the oldest version of doc w/id=1")
      (is (= nil (sut/q old-db "c"))
          "Old db doesn't know about doc with id=c"))

    (testing "Current database"
      (is (= 7 (-> current-db (sut/q 1) :b))
          "This is the newest version of doc w/id=1")
      (is (= current-doc (sut/q current-db "c"))
          "Current db returns current c doc"))

    (is (= new-doc (sut/q new-db "c")) "New db returns new c doc")

    (delete-meta new-db)
    (io/delete-file "resources/test/tmp-test.csv")))

;; TODO: The snippet below should show all lines of the index for all
;; lines in the database, not just the newest lines per doc!
;;
;; The serialization of the index come from a map which is not ordered..
;; It should come from the input file instead..
;; This means the index should be written line by line, when building the
;; index in the first place, instead of written when the entire index is
;; realized (so writing the index is lazy as well as the reading).
;;
;; Actually: Perhaps have a index log, and a shortened index!? Because
;; otherwise we can't lazily read the unique ID's of the database via the
;; index! :(
;; So:
;; 1: index-log (representing _all_ lines in the document log), and
;; 2: index (could be read from the end of the index!!!, building up an
;;    index of IDs to offset/length, ignoring ID's that have already been
;;    added to the index - which is exactly the opposite as what we have
;;    today.!
#_(let [db (nddb/db {:filename "resources/test/test.csv"
                     :col-separator ","
                     :id-path :a
                     :log-limit 2})]
    ;;(-> db :index deref)
    ;;(nddb/q db 1)
    (with-open [r (-> db ndio/serialized-db-filepath io/reader)]
      (->> r line-seq
           rest
           (mapv ndio/str->))))
