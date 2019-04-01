[![License](https://img.shields.io/badge/License-EPL%202.0-blue.svg)](https://www.eclipse.org/legal/epl-v20.html) [![Clojars Project](https://img.shields.io/clojars/v/luposlip/ndjson-db.svg)](https://clojars.org/luposlip/ndjson-db)

# ndjson-db

```clojure
[luposlip/ndjson-db "0.1.2"]
```

Clojure library for using (huge) .ndjson files as lightning fast databases.

## Usage

A very tiny *test database* resides in `resources/test/test.ndjson`.

It contains the following 3 documents, that has `"id"` as their unique IDs:

```json
{"id":1, "data": ["some", "semi-random", "data"]}
{"id":222, "data": 42}
{"id":333333,"data": {"datakey": "datavalue"}}
```

### Query Single Document

To find the data for the document with ID `222`, you can perform a `query-single`:

```clojure
(db/query-single
 {:id-fn-key :by-id
  :filename "resources/test/test.ndjson"}
 222)
```

### Query Multiple Documents

If you use the multiple select interface, the function is added to the internal
ID-function repository:

```clojure
(db/query
 {:id-fn-key :by-id
  :id-fn #(Integer. ^String (second (re-find #"^\{\"id\":(\d+)" %)))
  :filename  "resources/test/test.ndjson"}
 [333333 1 77])
```

### It keeps!

NB: The above returns only 2 documents, since there is no document with ID 77.
This is a design decision, as the documents themselves still contain the ID.

In a pipeline you'll be able to give lots of IDs to `query`, and filter down
on documents that are actually represented in the database.

If you want to have an option to return `nil` in this case, let me know by
creating an issue (or a PR).

### The ID function

The ID functions adds unlimited flexibility as how to uniquely identify each
document.

As you can see you can specify a function to use for creating the index. Since
functions in Clojure cannot be uniquely identified at runtime, you refer to it
by key.

The framework keeps track of registered functions that can be used to create
the index.

In the `:by-id` example above, the value of `"id"` is used as a unique ID to
built up the database index.

#### Parsing JSON documents

If you use very large databases, it makes sense to think about performance in
your ID function. In the above example a regular expression is used to find
the value of `"id"`, since this is faster than parsing JSON objects to EDN and
querying them as maps.

#### Return value

Furthermore the return value of the function is (almost) the only thing being
stored in memory. Because of that you should opt for as simple data values
as possible. In the above `:by-id` example this is the reason for the parsing
to Integer instead of keeping the String value.

Also note that the return value is the same you should use to query the
database. Which is why the inputs to `query-single` and `query` are integers.

Refer to the test for more details.

### Clear indices

If you want to clear an index use the function `clear-index!` like this:

```clojure
(ndjson-db.core/clear-index!
  {:id-fn-key :by-id
   :filename "resources/test/test.ndjson"})
```

If you want to clear all indices, use `clear-all-indices!`.

The above mentioned clearing functions are particularly useful in development and
test scenarios.

## Real use case: Verified Twitter Accounts

To test with a real database, download all verified Twitter users from here:
https://files.pushshift.io/twitter/TU_verified.ndjson.xz

Put the file somewhere, i.e. `path/to/TU_verified.ndjson`, and run the
following in a repl:

```clojure
(time 
   (def katy-gaga-gates-et-al
     (doall
      (db/query
       {:id-name "screen_name" 
        :filename "path/to/TU_verified.ndjson"}
       ["katyperry" "ladygaga" "BillGates" "ByMikeWilson"]))))
```


## Performance

The extracted .ndjson files is 513 MB (297,878 records).

On my laptop the initial build of the index takes around 3 seconds, and the subsequent
query of the above 3 verified Twitter users takes around 1 millisecond
(specs: Intel® Core™ i7-8750H CPU @ 2.20GHz × 6 cores with 31,2 GB RAM, SSD HD).

In real usage scenarios, I've used 2 databases simultaneously of sizes 1.6 GB and
2.0 GB, with no problem or penalties at all (except for the relatively small size of
the indices of course).

Since the database uses disk random access, SSD speed up the database significantly.


## License

Copyright © 2019 Henrik Mohr

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
