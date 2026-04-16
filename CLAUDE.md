# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo build                   # build the library
cargo test --all-features     # run all tests
cargo clippy --all-features   # lint
cargo run --example tagged    # run the tagged index example
cargo run --example numdex    # run the numeric index example
```

To run a single test:
```bash
cargo test test_name --all-features
```

Tests create temporary databases under `tmp/test/`.

## Architecture

Mokuroku is a secondary index library for RocksDB, inspired by PouchDB's map/reduce views. The library maintains secondary indices automatically as documents are stored.

### Core flow

1. Application opens a `Database` with a path, a list of view names, and a `ByteMapper` function
2. `put()` stores a document and invokes the `ByteMapper` for each registered view, calling `Document::map()` to emit index entries
3. Index entries are stored in RocksDB column families named `mrview-<view_name>`
4. Index keys are composite: `[index_key + separator + primary_key]` to enforce uniqueness across documents with the same index key

### Key types

- **`Document` trait** (`src/lib.rs`): Application types implement this — `from_bytes()` for deserialization, `to_bytes()` for serialization, and `map()` to emit index key/value pairs via `Emitter`
- **`ByteMapper`**: A function the application provides that receives raw bytes, deserializes them into a concrete type, and calls `map()`. This is how the library avoids depending on any specific serialization format.
- **`Database`**: The main wrapper around a `rocksdb::DB` instance. Manages index column families and coordinates `put`/`get`/`delete`/query operations.
- **`Emitter`**: Passed to `Document::map()` — call `emit(key, value)` to add an entry to a view's index
- **`QueryResult`**: Returned from queries — contains `key` (index key), `value` (emitted value), and `doc_id` (primary key)

### Feature flags

- **`hat`**: Enables `get_all_keys_hat()` returning a `HashedArrayTree` instead of `Vec` for reduced memory overhead on large key sets
- **`anyhow`**: Auto-converts `anyhow::Error` into `mokuroku::Error`
- **`serde_cbor`**: Auto-converts `serde_cbor::Error` into `mokuroku::Error`

### `base32` module

`src/base32.rs` implements base32hex encoding that preserves bitwise sort order. This allows numeric values (integers, dates, etc.) to be stored as string index keys while maintaining correct range query ordering — see `examples/numdex.rs` for usage.
