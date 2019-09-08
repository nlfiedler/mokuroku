# Change Log

All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).
This file follows the convention described at
[Keep a Changelog](http://keepachangelog.com/en/1.0.0/).

## [Unreleased]
### Added
- `query_exact()` to return results matching the entire index key.
- `query_all_keys()` to return results that have all of the given keys.
- `delete_index()` to completely remove an index from the database.
- `index_cleanup()` to remove unknown mokuroku column families.

## [0.2.0] - 2019-09-07
### Changed
- Detect and prune stale index entries on query.
- Index format changed significantly, indices must be rebuilt.
- Removed dependency on ulid crate.

## [0.1.0] - 2019-08-27
### Changed
- Initial release
