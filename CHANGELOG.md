# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [Unreleased]

### TODO

## [0.9.0-beta5] - 2023-04-20

Add convenience compression functions

## [0.9.0-beta3+4] - 2023-03-23+27

Minor refactoring


## [0.9.0-beta2] - 2023-03-23

### Added

- `nd-db.compress` namespace containing input- and output-stream convenience fns


## [0.9.0-beta1] - 2023-03-19

### Added

- Support for CSV/TSV files as databases

### TODO

- Revamp documentation

## [0.9.0-alpha6] - 2023-03-19

### Added

Utility function `nd-db.convert/upgrade-nddbmeta!` converts your old pre-v0.9.0
nddbmeta files to the new format, and keeps the old under the same name with
`_old` appended to the file name.

### Enhanced

Internally the database is now no longer a future. Instead the :index is a
delay. This means immediate initialization of the db value, and that the
:index doesn't get realized until you start querying.

This also means that the `lazy-docs` and `lazy-ids` make even better sense
if you just want to traverse the database sequentially, because in that case
you're not using the realized index at all.

The external API for the library is unchanged. You initialize the database
value in the same way, and you query it the same way too.

## [0.9.0-alpha5] - 2023-03-17

### Fixed

- `lazy-ids` failed in some cases when moving index

## [0.9.0-alpha4] - 2023-03-17

### Fixed

- `lazy-ids` now work when moving nddbmeta file around (i.e. with the db file)

### Changed

- Default now is to generate the index in the same folder as the database. Previously the default was the filesystem temp folder.
- nddbmeta files now only contain the serialized filename, as opposed to before where it was the complete path.

### Added

- Reader for compressed non-nippy nd* files

## [0.9.0-alpha3] - 2023-03-09

### Enhanced

- Parallelized index-creation - takes 2/3 less time than before (mbp m1)!
- Potentially more stable serialization of index (flushing every 1000 lines)

### TODO

- `lazy-ids` has internal `BufferedReader`. Should be passed from `with-open`.
- conversion function for pre-v0.9.0 `.nddbmeta` files.
- skip the realization of the index when generating the db value (= refactor)

## [0.9.0-alpha2] - 2023-03-08

### Changed

`lazy-docs` now works with eager indexes:

``` clojure
(lazy-docs nd-db)
```

Or with lazy indexes:

``` clojure
(with-open [r (nd-db.index/reader nd-db)]
  (->> nd-db
       (lazy-docs r)
       (drop 1000000)
       (filter (comp pos? :amount))
       (sort-by :priority)
       (take 10)))
```

NB: For convenience this also works, without any penalty:

``` clojure
(with-open [r (nd-db.index/reader nd-db)]
  (->> r
       (lazy-docs nd-db)
       ...
       (take 10)))
```


### TODO

Still need to make the conversion function for pre-v0.9.0 `.nddbmeta` files.

## [0.9.0-alpha1] - 2023-03-07

WIP! `lazy-docs` might change signature when using the new `index-reader`!

### (almost) breaking changes

- new format for metadata/index `.nddbmeta` file

The new format makes it much faster to initialize and sequentially read through
the whole database. The change will make the most impact for humongous databases
with millions of huge documents.

Old indexes will not be readable anymore. Good news is that there will be a new
`nd-db.convert/upgrade-nddbmeta!` utility function, which can converts your old
file to the new format, and overwrite it.

The downside to the support for laziness is the size of the meta+index files,
which in my tested scenarios have grown with 100%. This means a database
containing ~300k huge documents (of 200-300Kb each in raw JSON/EDN form) has
grown form ~5MB to ~10MB.

This is not a problem at all in real life, since when you need the realized
in-memory index (for ad-hoc querying by ID), it still consumes the same amount
of memory as before (in the above example ~3MB).

And compared to the database it describes it's nothing - the above mentioned
index is for a `.ndnippy` database of 16.8GB.

### Other changes

- because of the change to the metadata format, the `lazy-docs` introduced with
`v0.8.0` is now much more efficient. Again this is most noticable when you need
to read sequentially through parts of a huge database.

### Removed

Dependency `buddy/buddy-core` not needed anymore. Instead using built-in similar
functionality from `com.taoensso/nippy`.

## [0.8.0] - 2023-02-28

### Added

- Utility function to get lazy seq of all indexed IDs: `nd-db.io/lazy-docs`

## [0.7.2] - 2022-12-07

- Updated nippy

## [0.7.1] - 2022-08-03

- Bugfix release, downgrade nippy

Using projects couldn't compile nd-db with nippy version 3.2.0

## [0.7.0] - 2022-08-03

- Make serialized databases portable (not bound to a specific filesystem path)
- Support for clojure 1.11 keyword function parameters: https://clojure.org/news/2021/03/18/apis-serving-people-and-programs
- Upgrade dependencies

## [0.6.3] - 2022-04-07

- Upgrade Clojure 1.10.3 -> 1.11.1
- Upgraded other dependencies
- Minor optimizations

## [0.6.2] - 2021-09-30

Fix issue when creating index for ndjson/ndedn

## [0.6.1] - 2021-09-15

Eliminate a reflective call when serializing the database.

## [0.6.0] - 2021-09-06

`0.6.0` - introducing `.ndnippy`!

### Added

Now you can use `.ndnippy` as database format. It's **MUCH** faster to load than
`.ndjson` and `.ndedn`, meaning better query times. Particularly when querying multiple documents at once.

Also a new `util` namespace lets you convert from `.ndjson` and `.ndedn` to `.ndnippy`.

`.ndnippy` - like `.ndedn` isn't really a standard. But it probably should be. I implemented the encoding for
`.ndnippy` myself, it's somewhat naive, but really fast anyhow. If you have ideas on how to make it even
fast, let me know. Because version `0.6.0` introduces the `.ndnippy` format, it may change several times in the
future, possibly making old `.ndnippy` files incompatible with new versions. Now you're warned. Thankfully the
generation of new `.ndnippy` files is quite fast.

NB: `.ndnippy` isn't widely used (this is - as far as [I](https://github.com/luposlip) know, the first and only use), and probably isn't a good distribution format, unless you can distribute the `nd-db` library with it.

NB: For small documents (like the ones in the test samples), `.ndnippy` files are actually bigger than their
json/edn alternatives. Even the Twitter sample `.ndjson` file mentioned in the `README` becomes bigger as
`.ndnippy`. With the serialization mechanism used right now, the biggest benefits are when the individual documents
are huge (i.e. 10s of KBs). We've done experiments with methods that actually makes the resulting size the same as
the input, even for small documents. But there's a huge performance impact to using that, which is counter productive.

## [0.5.2] - 2021-09-01

### Fixed

- Bug when using new :id-rx-str as input

## [0.5.1] - 2021-08-26

### Fixed

- Eliminate reflective call when querying file

## [0.5.0] - 2021-08-26

### Added

- Persist the processed index to disk for fast re-initialization
  - Saves to temp filesystem folder (default)
  - optionally a different folder to persist index between system reboots
  - Uses filename, content and optionally regex-string to name the index file

## [0.4.0] - 2021-06-24

`0.4.0` - simpler and smaller!

### Added
- Auto-infer `:doc-type` from db file extension (`*.ndedn` -> `:doc-type :edn`)
  - This means you have to use either db extension `.ndedn`|`.ndjson` or `:doc-type :json`|`:edn`
  - Defaults to `:json` if extension is unknown and `:doc-type` isn't set
- Laying the groundwork for improved indexing performance via parallelization.
  - Need more work to limit memory consumption for huge databases, before enabling it

### Breaking Change!
- Rename core namespace to `nd-db.core`, and the library from `luposlip/ndjson-db` to `com.luposlip/nd-db`!

### Changed
- Removed core.memoize and timbre (not used anymore)
  - Smaller deployable!
- Updated clojure (-> 1.10.3)

## [0.3.0] - 2021-06-09

### Added
- Add support for the `.ndedn` file format, where all lines are well formed EDN documents.

## [0.2.2] - 2021-01-12

### Changed
- Updated depencies (timbre, memoize, cheshire)

## [0.2.1] - 2019-08-29

### Changed
- Updated depencies (clojure, cheshire, memoize)

## [0.2.0] - 2019-05-06

### Added
- Completely revamped API
- `clear-all-indices!!` -> `clear-all-indexes!!`
- Using [timbre](https://github.com/ptaoussanis/timbre) for logging
- Now using Apache License, Version 2.0 (instead of Eclipse Licence 2.0)

## [0.1.2] - 2019-04-01

### Added
- API for using default id-fn for querying by json name with type string or integer
- Added `clear-all-indices!!` and `clear-index!`
- Added documentation in README

## [0.1.1] - 2019-03-03

### Added
- Enhanced API for lazy/streaming usage

## [0.1.0] - 2019-03-03

### Added
- Initial public release
- Example on how to query huge datasets
