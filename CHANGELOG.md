# Change Log
All notable changes to this project will be documented in this file, which follows the guidelines
on [Keep a CHANGELOG](http://keepachangelog.com/). This project adheres to
[Semantic Versioning](http://semver.org/).

## [Unreleased]

## [6.4.0] - 2019-11-13
### Added
- Enhanced unit tests for anonymisation module

### Changed
- Update framework to 6.4.0
- Update event-stroe to 2.4.0
- Update utilities to 1.24.3

## [6.1.2] - 2019-10-14
### Fixed
- Date time pattern to accommodate for missing seconds entry

## [6.1.1] - 2019-10-09
### Fixed
- Running out of database connections due to streams not getting closed correctly
- Database state not rolling back due to exceptions getting suppressed and not propagating correctly

## [6.1.0] - 2019-10-07
### Fixed
- Email address generation when anonymising

## [6.0.1] - 2019-09-25
### Changed
- Update framework-api to 4.1.0
- Update framework to 6.0.16
- Update event-store to 2.0.22

## [6.0.0] - 2019-09-12
### Changed
- Upgrade to framework 6
- Truncate pre_publish_queue table after transformation is complete
- Rebuild published_event table after transformation is complete
- Update framework-api to 4.0.1
- Update framework to 6.0.14
- Update event-store to 2.0.15
- Update common-bom to 2.4.0
- Update utilities to 1.20.2
- Update test-utils to 1.24.3

## [5.3.1] - 2019-07-17
### Added
- Retry mechanism when performing stream operations such as append / move or clone

## [5.3.0] - 2019-06-27
### Fixed
- Reverting thorntail container related changes and using swarm instead
### Added
- Added a module to support anonymisation of events (currently works with only active streams).  Also, list of string patterns used to apply anonymisation will be expanded

### Changed
- Update event-store to 1.1.9

## [5.2.1] - 2019-03-21
### Changed
- Update event-store to 1.1.8
- Update utilities to 1.16.2 

## [5.2.0] - 2019-03-20
- Add support for linked event synch with event log after transformation

## [5.1.0] - 2018-12-12

### Fixed
- Issue where the creation date in an event to be transformed was being overwritten on transformation by the current date  

## [5.0.0] - 2018-11-20

### Changed
- Upgraded framework to 5.0.4
- Moved integration tests to using Postgres rather than H2 

## [4.1.0] - 2018-10-30

### Changed
- fix stream closing issue. 

# [4.0.0] - 2018-10-25

### Added
- Feature to specify transformation pass value so that transformations can be executed sequentially
- Support for moving transformed events to new or existing streams

## [3.0.0] - 2018-08-30

### Changed
- Upgraded to [microservice-framework 4.3.2](https://github.com/CJSCommonPlatform/microservice_framework/releases/tag/release-4.3.2)

## [2.1.0] - 2018-07-17

### Changed
- fix to remove the backup events from event log when backup is not to be retained

## [2.0.0] - 2018-06-21

### Added
- feature to specify actions on a stream like transformation, deactivation or backup to be retained

## [1.1.0] - 2018-06-01

### Added
- Support for archiving streams without transformations or cloning

## [1.0.1] - 2018-02-07

### Changed
- No functional changes; POM fixed for artefact deployment to Bintray

## [1.0.0] - 2018-01-19

### Added
- Initial implementation of the stream-transformation tool
- Integration test
- Performance test (disabled by default)
- _stream-transformation-test/sample-transformations_ module as an example

### Changed
- Updated [common-bom 1.22.0](https://github.com/CJSCommonPlatform/maven-common-bom) 
- Updated [microservice-framework 3.0.0](https://github.com/CJSCommonPlatform/microservice_framework) 
- Updated [utilities 1.11.0](https://github.com/CJSCommonPlatform/utilities) 
- Updated [test-utils 1.16.0](https://github.com/CJSCommonPlatform/test-utils) 

### Fixed
- Database connection exhaustion error

[Unreleased]: https://github.com/CJSCommonPlatform/stream-transformation-tool/compare/release-1.1.0...HEAD
[1.1.0]: https://github.com/CJSCommonPlatform/istream-transformation-tool/compare/release-1.0.1...release-1.1.0
[1.0.1]: https://github.com/CJSCommonPlatform/stream-transformation-tool/compare/release-1.0.0...release-1.0.1
[1.0.0]: https://github.com/CJSCommonPlatform/stream-transformation-tool/commits/release-1.0.0
