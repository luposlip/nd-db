# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [Unreleased]

### TODO

- Utility function to get lazy seq of all indexed IDs
- Persist index in temp file system for fast future initialization

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

[Unreleased]: https://github.com/luposlip/nd-db/compare/0.4.0...HEAD
[0.4.0]: https://github.com/luposlip/nd-db/compare/0.3.0...0.4.0
[0.3.0]: https://github.com/luposlip/nd-db/compare/0.2.2...0.3.0
[0.2.2]: https://github.com/luposlip/nd-db/compare/0.2.1...0.2.2
[0.2.1]: https://github.com/luposlip/nd-db/compare/0.2.0...0.2.1
[0.2.0]: https://github.com/luposlip/nd-db/compare/0.1.2...0.2.0
[0.1.2]: https://github.com/luposlip/nd-db/compare/0.1.1...0.1.2
[0.1.1]: https://github.com/luposlip/nd-db/compare/0.1.0...0.1.1
