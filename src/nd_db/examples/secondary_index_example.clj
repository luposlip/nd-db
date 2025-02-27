(ns nd-db.examples.secondary-index-example
  (:require [nd-db.core :as nddb]
            [nd-db.secondary-index :as sidx]))

(defn twitter-example-with-secondary-indexes
  "Demonstrate using nd-db with secondary indexes on Twitter data"
  []
  (println "Creating database with secondary indexes...")
  
  ;; Define secondary index extractors
  (def extractors
    {:verified #(get % "verified")
     :followers_count #(get % "followers_count")
     :screen_name_length #(count (get % "screen_name"))
     :created_year #(when-let [created (get % "created_at")]
                      (when (string? created)
                        (Integer/parseInt (subs created (- (count created) 4)))))})
  
  ;; Create database with secondary indexes
  (def twitter-db
    (nddb/db {:id-name "screen_name"
              :id-type :string
              :filename "path/to/TU_verified.ndjson"
              :secondary-indexes extractors
              :range-indexes #{:followers_count :screen_name_length :created_year}}))
  
  ;; Example queries using secondary indexes
  (println "\nQuery by verified status:")
  (def verified-users (sidx/q-by twitter-db :verified true))
  (println "Found" (count verified-users) "verified users")
  
  (println "\nQuery users with followers between 1M and 2M:")
  (def popular-users (sidx/q-range twitter-db :followers_count 1000000 2000000))
  (println "Found" (count popular-users) "users with 1M-2M followers")
  
  (println "\nQuery users with screen names of exactly 10 characters:")
  (def users-with-10-char-names (sidx/q-by twitter-db :screen_name_length 10))
  (println "Found" (count users-with-10-char-names) "users with 10-character screen names")
  
  (println "\nQuery users created in 2010 who are verified:")
  (def users-2010-verified 
    (sidx/q-where twitter-db {:created_year 2010 :verified true}))
  (println "Found" (count users-2010-verified) "verified users created in 2010")
  
  (println "\nAll done!"))

;; Example of using with a CSV database
(defn csv-example-with-secondary-indexes
  "Demonstrate using nd-db with secondary indexes on CSV data"
  []
  (println "Creating CSV database with secondary indexes...")
  
  ;; Define CSV row parsers and secondary index extractors
  (defn parse-csv-row [row]
    (let [[id name age country salary] (clojure.string/split row #",")]
      {:id id
       :name name
       :age (Integer/parseInt age)
       :country country
       :salary (Double/parseDouble salary)}))
  
  (def csv-extractors
    {:country #(:country %)
     :age #(:age %)
     :salary #(:salary %)})
  
  ;; Create database with secondary indexes
  (def csv-db
    (nddb/db {:id-fn #(-> % parse-csv-row :id)
              :doc-type :csv
              :filename "path/to/employees.csv"
              :secondary-indexes csv-extractors
              :range-indexes #{:age :salary}}))
  
  ;; Example queries using secondary indexes
  (println "\nQuery by country:")
  (def us-employees (sidx/q-by csv-db :country "USA"))
  (println "Found" (count us-employees) "US employees")
  
  (println "\nQuery employees between 30-40 years old:")
  (def mid-career-employees (sidx/q-range csv-db :age 30 40))
  (println "Found" (count mid-career-employees) "employees between 30-40")
  
  (println "\nQuery high-earners in Canada:")
  (def canadian-high-earners 
    (sidx/q-where csv-db {:country "Canada" :salary 100000.0}))
  (println "Found" (count canadian-high-earners) "high-earning Canadian employees")
  
  (println "\nAll done!"))