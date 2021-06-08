![Clojure CI](https://github.com/luposlip/ndjson-db/workflows/Clojure%20CI/badge.svg?branch=master) [![Clojars Project](https://img.shields.io/clojars/v/luposlip/ndjson-db.svg)](https://clojars.org/luposlip/ndjson-db) [![Dependencies Status](https://versions.deps.co/luposlip/ndjson-db/status.svg)](https://versions.deps.co/luposlip/ndjson-db) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# ndjson-db

```clojure
[luposlip/ndjson-db "0.3.0"]
```

Clojure library for using (huge) [.ndjson](http://ndjson.org/) files as lightning fast databases.

## Usage

A very tiny *test database* resides in `resources/test/test.ndjson`.

It contains the following 3 documents, that has `"id"` as their unique IDs:

```json
{"id":1, "data": ["some", "semi-random", "data"]}
{"id":222, "data": 42}
{"id":333333,"data": {"datakey": "datavalue"}}
```

### Create Database

Since version `0.2.0` you need to create a database var before you can use the
database. Behind the scenes this creates an index for you in a background thread.

```clojure
(def db (ndjson-db.core/db {:id-fn #(Integer. ^String (second (re-find #"^\{\"id\":(\d+)" %))))
                            :filename "resources/test/test.ndjson"}))
```

If you want a default `:id-fn` created for you, use the `:id-name` together with `:id-type` and/or `:source-type`. Both `:id-type` and `:source-type` can be `:string` or `:integer`. `:id-type` is the target type of the indexed ID, whereas `:source-type` is the type in the source `.ndjson` database file. `:source-type` defaults to `:id-type`, and `:id-type` defaults to `:string`:

```clojure
(def db (ndjson-db.core/db {:id-name "id"
                            :id-type :integer
                            :filename "resources/test/test.ndjson"}))
```


### Query Single Document

With a reference to the `db` you can query the database.

To find the data for the document with ID `222`, you can perform a `query-single`:

```clojure
(ndjson-db.core/q db 222)
```

### Query Multiple Documents

You can also perform multiple queries at once. This is ideal for a pipelined scenario,
since the return value is a lazy seq:

```clojure
(ndjson-db.core/q db [333333 1 77])
```

### It keeps!

NB: The above query for multiple documents, returns only 2 documents, since there is
no document with ID 77. This is a design decision, as the documents themselves still
contain the ID.

In a pipeline you'll be able to give lots of IDs to `q`, and filter down on documents
that are actually represented in the database.

If you want to have an option to return `nil` in this case, let me know by
creating an issue (or a PR).

### The ID function

The ID functions adds "unlimited" flexibility as how to uniquely identify each
document. You can choose a single attribute, or a composite. It's entirely up to
you when you implement your ID function.

In the example above, the value of `"id"` is used as a unique ID to
built up the database index.

#### Parsing JSON documents

If you use very large databases, it makes sense to think about performance in
your ID function. In the above example a regular expression is used to find
the value of `"id"`, since this is faster than parsing JSON objects to EDN and
querying them as maps.

#### Return value

Furthermore the return value of the function is (almost) the only thing being
stored in memory. Because of that you should opt for as simple data values
as possible. In the above example this is the reason for the parsing to `Integer`
instead of keeping the `String` value.

Also note that the return value is the same you should use to query the
database. Which is why the input to `q` are `Integer` instances.

Refer to the test for more details.

### Clear indices

If you want to force the recreation of an index, use the function `clear-index!`
like this:

```clojure
(ndjson-db.core/clear-index! db)
```

With `clear-all-indexes!!` you can clear all indices across all databases currently in use.

The above mentioned clearing functions are particularly useful in development and
test scenarios.

NB: The framework keeps a live index for each new database you create with the `db`
function, as long as the resulting index return a different value for the first couple of
entries in the database. If you're not aware of this, it could theoretically lead to a
high memory usage in development scenarios, where you try out a lot of different ID
functions for the same (large) database(s).

To avoid this you could always use clear the index/indices before you try out a new ID
function. Alternatively you can just restart your repl every now and then.

## Real use case: Verified Twitter Accounts

To test with a real database, download all verified Twitter users from here:
https://files.pushshift.io/twitter/TU_verified.ndjson.xz

Put the file somewhere, i.e. `path/to/TU_verified.ndjson`, and run the
following in a repl:

```clojure
(time 
   (def katy-gaga-gates-et-al
     (doall
      (ndjson-db.core/q
       (ndjson-db.core/db {:id-name "screen_name" 
                           :filename "path/to/TU_verified.ndjson"})
       ["katyperry" "ladygaga" "BillGates" "ByMikeWilson"]))))
```

## Performance

The extracted .ndjson files is 513 MB (297,878 records).

On my laptop the initial build of the index takes around 3 seconds, and the subsequent
query of the above 3 verified Twitter users takes around 1 millisecond
(specs: Intel® Core™ i7-8750H CPU @ 2.20GHz × 6 cores with 31,2 GB RAM, SSD HD).

In real usage scenarios, I've used 2 databases simultaneously of sizes 1.6 GB and
43.0 GB, with no problem or performance penalties at all (except for the relatively small
size of the in-memory indices of course). Indexing the biggest database of 43GB took less
than 2 minutes.

Since the database uses disk random access, SSD speed up the database significantly.


## Copyright & License

Copyright (C) 2019-2021 Henrik Mohr

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

            http://www.apache.org/licenses/LICENSE-2.0
            
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
