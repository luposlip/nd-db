![Clojure CI](https://github.com/luposlip/nd-db/workflows/Clojure%20CI/badge.svg?branch=master) [![Clojars Project](https://img.shields.io/clojars/v/com.luposlip/nd-db.svg)](https://clojars.org/com.luposlip/nd-db) [![Dependencies Status](https://versions.deps.co/luposlip/nd-db/status.svg)](https://versions.deps.co/luposlip/nd-db) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# nd-db

```clojure
[com.luposlip/nd-db "0.9.0-alpha7"]
```

_Newline Delimited (read-only) Databases!_

Clojure library that treats lines in newline delimited (potentially humongous) files as simple (thus lightening fast) databases.

`nd-db` currently works with JSON documents in [.ndjson](http://ndjson.org/) files, and EDN documents in `.ndedn`. It also supports binary [nippy](https://github.com/ptaoussanis/nippy) encoded EDN documents.

## Usage

A very tiny *test database* resides in `resources/test/test.ndjson`.

It contains the following 3 documents, that has `"id"` as their unique IDs:

``` json
{"id":1, "data": ["some", "semi-random", "data"]}
{"id":222, "data": 42}
{"id":333333,"data": {"datakey": "datavalue"}}
```

### Create Database

Since version `0.2.0` you need to create a database var before you can use the
database. Behind the scenes this creates an index for you in a background thread.

```clojure
(def db
  (nd-db.core/db
    :id-fn #(Integer. ^String (second (re-find #"^\{\"id\":(\d+)" %))))
    :filename "resources/test/test.ndjson"))
```

If you want a default `:id-fn` created for you, use the `:id-name` together with `:id-type` and/or `:source-type`. Both `:id-type` and `:source-type` can be `:string` or `:integer`. `:id-type` is the target type of the indexed ID, whereas `:source-type` is the type in the source `.ndjson` database file. `:source-type` defaults to `:id-type`, and `:id-type` defaults to `:string`:

```clojure
(def db
  (nd-db.core/db
    :id-name "id"
    :id-type :integer
    :filename "resources/test/test.ndjson"}))
```

### EDN

If you want to read a database of EDN documents, just use `:doc-type :edn`. Please note that the standard `:id-name` and `:id-type` parameters doesn't (as of v0.3.0) work with EDN, hence you need to implement the `:id-fn` accordingly.

### Nippy

Nippy can be used also. Since this is a binary standard, you'd probably start out with a `.ndjson` or `.ndedn` file, and convert it to `.ndnippy` via the `nd-db.convert` namespace. "Why?" you ask. Because of speed. Especially for big documents (10-100s of KBs) the parsing make a huge difference.

Because the nippy-serialized documents are "just" EDN, you can simply give a path for the ID with the `:id-path` parameter. Or of course use the mighty `:id-fn` instead.

There's a sample `resources/test/test.ndnippy` database representing the same data as the `.ndjson` and `.ndedn` samples.

### Query Single Document

With a reference to the `db` you can query the database.

To find the data for the document with ID `222`, you can perform a `query-single`:

```clojure
(nd-db.core/q db 222)
```

### Query Multiple Documents

You can also perform multiple queries at once. This is ideal for a pipelined scenario,
since the return value is a lazy seq:

```clojure
(nd-db.core/q db [333333 1 77])
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

## Laziness!

Since v0.9.0 both the index and the documents can be retrieved in a truly lazy fashion.

Getting the those lazy seqs are possible, since v0.9.0 introduces a new meta+index file format,
that makes lazy traversal of the index possible.

The downside to the support for laziness is the size of the meta+index files,
which in my tested scenarios have grown with 100%. This means a `.ndnippy`
database of 16.8GB containing ~300k huge documents (of 200-300Kb each in raw
JSON/EDN form) has grown form ~5MB to ~10MB.

This is not a problem at all in real life, since when you need the realized
in-memory index (for ad-hoc querying by ID), it still consumes the same amount
of memory as before (in the above example ~3MB).

### Lazy IDs

Here's an example on how to get and use the the lazy IDs:

``` clojure
(with-open [r (nd-db.index/reader my-db)]
  (->> r
       nd-db.core/lazy-ids
       (drop 100000)
       (take 100)
       (sort >)
       first))
```

### Lazy Documents

NB: This currently (v0.9.0) only works for `.ndnippy` databases!

Getting the documents contained in a `nippy` document based database, is just as
simple as getting the IDs:

``` clojure
(with-open [r (nd-db.index/reader my-db)]
  (->> r
       (nd-db.core/lazy-docs my-db)
       (drop 100000)
       (take 100)
       (sort-by :some-value >)
       first))
```

The above example spends ~1ms on my laptop (mbp m1 pro) per dropped document, which isn't a lot.
But if you don't actually need (most of) the documents, it's much faster to use the IDs as entry,
like in this example:

``` clojure
(with-open [r (nd-db.index/reader my-db)]
  (->> r
       nd-db.core/lazy-ids
       (drop 100000)
       (take 100)
       (q my-db)
       (sort-by :some-value >)
       first))
```

## Persisting the database index

From v0.5.0 the generated index will be persisted to the temporary system folder on disk.
This is a huge benefit if you need to use the same database multiple times, after throwing
away the reference to the parsed database, since it takes much less time to read in the index
as compared to parsing the database file.

For small files (like the sample databases found in this repository) it doesn't really make a difference.
But for huge files, it makes an immense difference. The bigger the databases, the bigger the individual
documents and the more complex the parsing of these documents are (to find the unique ID), the bigger
the difference. For a database file of 4.7GB the difference is 47s vs 90ms, or **~500 times faster**!!

If you want to keep the serialized meta+index file (`*.nddbmeta`) between system reboots, you should move it
to another folder. You do that by passing `:index-folder` to the `db` function.

If for some reason you don't want to persist the index - e.g. there's no storage attached to a docker
container or serverless system - you can inhibit the persistence by setting param `:index-persist?`
to `false`.

From `v0.9.0` onwards the index is generated in parallel. This cuts two thirds of the processing time.

For more information on these and other parameters, see the source code for the `db` function in the
`core` namespace.

## Real use case: Verified Twitter Accounts

To test with a real database, download all verified Twitter users from here:
https://files.pushshift.io/twitter/TU_verified.ndjson.xz

Put the file somewhere, i.e. `path/to/TU_verified.ndjson`, and run the
following in a repl:

```clojure
(time
   (def katy-gaga-gates-et-al
     (doall
      (nd-db.core/q
       (nd-db.core/db {:id-name "screen_name"
	                   :id-type :string
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
size of the in-memory indexes of course). Indexing the biggest database of 43GB took less
than 2 minutes (NB: This is with a single core, and BEFORE 0.4.0).

Since the database uses disk random access, SSD speed up the database significantly.

** Update **

On a MacBook Pro M1 Pro with 32 GB memory, the querying takes around 0.5 ms!


## Copyright & License

Copyright (C) 2019-2023 Henrik Mohr

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

            http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
