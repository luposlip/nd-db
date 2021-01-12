# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [Unreleased]

### TODO

- Utility function to get lazy seq of all indexed IDs

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

[Unreleased]: https://github.com/luposlip/ndjson-db/compare/0.2.0...HEAD
[0.2.0]: https://github.com/luposlip/ndjson-db/compare/0.1.2...0.2.0
[0.1.2]: https://github.com/luposlip/ndjson-db/compare/0.1.1...0.1.2
[0.1.1]: https://github.com/luposlip/ndjson-db/compare/0.1.0...0.1.1
