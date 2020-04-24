# Change Log

All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).
This file follows the convention described at
[Keep a Changelog](http://keepachangelog.com/en/1.0.0/).

## Unreleased
### Changed
- Upgrade to `rust-rocksdb` 0.14.0 release.

## [2.1.0] - 2020-01-18
### Added
- Add `query_range()` to query an index for keys between A and B.
- Add `base32` module with base32hex implementation to support numeric keys.

## [2.0.0] - 2020-01-13
### Changed
- Rename `new()` to `open_default()`, and `with_opts()` to `open()`, to better
  reflect the RocksDB function names.
- Mark `ByteMapper` with `Send` and `Sync` to support concurrency.

## [1.0.1] - 2019-12-23
### Added
- Add `with_opts()` to provide custom database options.

## [1.0.0] - 2019-11-17
### Added
- `count_by_key()` to count number of index rows containing key.
- `count_all_keys()` to return number of occurrences of all keys.
### Changed
- **Index format has changed, indices must be rebuilt.**
- All query/count functions now panic if given an unknown index.
- Upgrade `rocksdb` crate to `0.13.0`, everything is mutable now.
- Use sequence number instead of timestamp for stale index detection.

## [0.3.0] - 2019-09-08
### Added
- `query_exact()` to return results matching the entire index key.
- `query_all_keys()` to return results that have all of the given keys.
- `delete_index()` to completely remove an index from the database.
- `index_cleanup()` to remove unknown mokuroku column families.

## [0.2.0] - 2019-09-07
### Changed
- **Index format has changed, indices must be rebuilt.**
- Detect and prune stale index entries on query.
- Removed dependency on ulid crate.

## [0.1.0] - 2019-08-27
### Changed
- Initial release
