# mokuroku

## Overview

This [Rust](https://www.rust-lang.org) crate is designed to provide a secondary index on top of the [RocksDB](https://rocksdb.org) key/value store, similar to what [PouchDB](https://pouchdb.com) does for [LevelDB](https://github.com/google/leveldb). Your application will provide implementations of the `Document` trait to suit the various types of data to be stored in the database, and this library will invoke the mapping function on those `Document` instances to produce index key/value pairs.

The behavior of this library is similar to PouchDB, albeit with an API suitable for the language. Unlike PouchDB, this library does not put any constraints on the format of the database records. As a result, the library relies on the application to provide the functions for deserializing records and invoking the `emit()` function with the secondary key and an optional value. To avoid unnecessary deserialization, the library will call `Document.map()` with each defined index name whenever the application calls the `put()` function on the `Database` instance.

### Classification

What this library does exactly: the indices managed by this library are "stand-alone", meaning they are not embedded within the database files (e.g. zone maps or bloom filters). Additionally, the index is updated in a lazy fashion, meaning that changes are appended rather than eagerly merged on update. This description from [3] nicely captures the overall performance:

> Here, each entry in the secondary indexes is a composite key consisting of
> (secondary key + primary key). The secondary lookup is a prefix search on
> secondary key, which can be implemented using regular range search on the
> index table. Here, writes and compactions are faster than "Lazy," but
> secondary attribute lookup may be slower as it needs to perform a range scan
> on the index table.

One difference from a simple composite index is that this library permits the application to emit a value along with the secondary key, in place of the `null` that would normally be the value in the secondary index.

### Contributions Are Welcome

While the author has read a couple of relevant research papers, he is not by any means an expert in database technology. Likewise, his Rust skills may not be all that impressive either. If you wish to contribute, by all means, please do. Thank you in advance.

## Building and Testing

### Prerequisites

* [Rust](https://www.rust-lang.org) stable (2024 edition)
* [Clang](https://clang.llvm.org/)

### Building and Testing

These commands will build the library and run the tests.

```shell
$ cargo clean
$ cargo build
$ cargo test
```

### Apple M1 Support

With the 0.16 release of [rust-rocksdb](https://crates.io/crates/rocksdb) this library supports building using the ARM64 target on the macOS platform since release **2.5.0**.

### macOS Setup

Installing XCode and the command line tools should be sufficient.

### Windows Setup

Installing Clang is easy with the command `winget install LLVM`, after which you need to add the path to the LLVM `bin` directory to your `PATH` environment variable (by default that will be in `C:\Program Files\LLVM\bin`).

## Examples

See the full example in `examples/tagged.rs`, which creates a RocksDB database, adds a few records,establishes a secondary index, and queries that index using specific key values.

```shell
cargo run --example tagged
```

An example demonstrating numeric indices, `examples/numdex.rs`, utilizes the bitwise sort order preservation of [base32hex](https://en.wikipedia.org/wiki/Base32#base32hex), which also has the benefit of making the numeric keys "safe" for the composite key format of the secondary index. The key in the `numdex` index is the UTC milliseconds, and the query is for assets updated within a specific date range. Both the index keys and the query keys must be encoded, and it helps for the numbers to be in [Big-endian](https://en.wikipedia.org/wiki/Endianness#Big-endian) order.

```shell
cargo run --example numdex
```

### Quick Example

This code snippet is lifted from the aforementioned example. It shows the most basic usage of opening the database, adding records, and querying an index. Examples of the functions for generating the index keys and values are in the `examples/tagged.rs` example code.

```rust
let db_path = "my_database";
let views = vec!["tags".to_owned()];
let dbase = Database::open_default(Path::new(db_path), views, Box::new(mapper)).unwrap();
let documents = [
    Asset {
        key: String::from("asset/blackcat"),
        location: String::from("hawaii"),
        tags: vec![
            String::from("cat"),
            String::from("black"),
            String::from("tail"),
        ],
    },
    // ...
];
for document in documents.iter() {
    let key = document.key.as_bytes();
    let _ = dbase.put(&key, document);
}

// querying the "tags" index for keyword "cat"
let result = dbase.query_by_key("tags", b"cat");
let iter = result.unwrap();
let results: Vec<QueryResult> = iter.collect();
for result in results {
    let doc_id = str::from_utf8(&result.doc_id).unwrap().to_owned();
    println!("query result key: {:}", doc_id);
}
```

## Features

### Optional features

Mokuroku supports several optional features to reduce the burden of using this crate with other popular crates.

* `anyhow`: Enable auto-conversion of `anyhow::Error` to `mokuroku::Error`
* `hat`: Enable `get_all_keys_hat()` which returns a `HashedArrayTree` in place of a `Vec`.
* `serde_cbor`: Enable auto-conversion of `serde_cbor::Error` to `mokuroku::Error`

## Design

### Terminology

Quick note on the terminology that this project uses. You may see the term _view_ used here and there. This is what [CouchDB](https://couchdb.apache.org) and PouchDB call the indices in their documentation. Given this crate attempts to operate in a similar fashion, it seems natural to use the same term. The function name `emit` also comes from the "map/reduce" API of CouchDB, and makes as much sense as anything else.

### Usage

The application will use the `mokuroku::Database` struct in place of `rocksb::DB`, as this crate will create and manage that `DB` instance. Since the crate is managing the secondary indices, it is necessary for the application to call the `put()` function on `Database`, rather than calling directly to `DB`. For those operations that should not affect any index, the application is free to get a direct reference to `DB` using the `Database.db()` function.

At startup, the application will create an instance of `Database` and provide three arguments.

1. Path to the database files, just as with `DB::open()`
1. Collection of index names that will be passed to `Document.map()`
1. A boxed function of type `mokuroku::ByteMapper`

The set of index names are those indices which the library will update every time `Database.put()` is called. That is, the implementation of `Document` that is passed to the `put()` call will have its `map()` invoked with each of the provided index names. Not every `Document` will emit index key/value pairs for every index. In fact, there is no requirement to emit anything, it is entirely application dependent. Similarly, a single invocation of a mapper may emit multiple values, which is demonstrated in the `tagged` example.

The `ByteMapper` is necessary when the library needs to rebuild an index. Since the library will be reading every record in the default column family, it does not know how to deserialize the records into the appropriate `Document` implementation. For this reason, the application must provide this function to recognize and deserialize records, and then emit index key/value pairs.

After setting up the database, the application may want to invoke `query()` on the database for each named index. This will cause the library to build the indices if they are missing, which will improve the response time of subsequent calls to `query()`.

### Data Model

The application defines the database record format; this library does not put any restrictions on the format of the keys or values. That is a big part of why the usage is slightly more complicated then something like PouchDB.

The library maintains the secondary indices in separate column families, whose names start with `mrview-` to avoid collision with any column family that the application may have created. For instance, if the application creates an index named **tags**, then the library will create a column family named `mrview-tags` and populate it with the values given to the `Emitter` passed to the implementations of `Document.map()` and `ByteMapper` defined by the application.

The index keys output by the application need not be unique. The library will append the data record primary key to ensure that no index entry will overwrite any other (the two keys are separated by a null byte; if you need to change this use the `Database.separator()` function). The application can emit an optional value for the index entry, whose format is entirely up to the application.

## References

In published order, the following papers were referenced during the design of this library, with the second being the most relevant.

* \[1\]: [Diff-Index: Differentiated Index in Distributed Log-Structured Data Stores](https://www.semanticscholar.org/paper/Diff-Index%3A-Differentiated-Index-in-Distributed-Tan-Tata/385d44dccb9c24a039d12c2eb2f011f5a057466d)
* \[2\]: [Efficient Secondary Attribute Lookup in NoSQL Databases](https://www.semanticscholar.org/paper/Efficient-Secondary-Attribute-Lookup-in-NoSQL-Qader-Cheng/f78c397df0f3296b97178f773514b7c6c8a99cf2)
* \[3\]: [Comparative Study of Secondary Indexing Techniques in LSM-based NoSQL Databases](https://www.semanticscholar.org/paper/A-Comparative-Study-of-Secondary-Indexing-in-NoSQL-Qader-Cheng/1c8b5b018f61c8170554a2cd38b689f3b1e5eab2)
