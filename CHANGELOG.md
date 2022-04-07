# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [Unreleased]

### TODO

- Utility function to get lazy seq of all indexed IDs

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
