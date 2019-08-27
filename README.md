# mokuroku

## Overview

This [Rust](https://www.rust-lang.org) crate is designed to provide a secondary
index on top of the [RocksDB](https://rocksdb.org) key/value store, much like
[PouchDB](https://pouchdb.com) does for LevelDB. The application provides
implementations of the `Document` trait to suit the various types of data to be
stored in the database, and this library will invoke a mapping function provided
by the application to produce index key/value pairs.

The design of this library is similar to PouchDB, albeit with an API suitable
for the language. Unlike PouchDB, however, this library does not put any
constraints on the format of the database records. As a result, the API relies
on the application to provide the functions for deserializing records and
invoking the `emit()` function with index key/value pairs. To avoid unnecessary
deserialization, the library will call `Document.map()` with each defined view
whenever the application calls the `put()` function on the `Database` instance.

## Building and Testing

### Prerequisites

* [Rust](https://www.rust-lang.org) stable (2018 edition)

### Building and Testing

These commands will build the library and run the tests.

```shell
$ cargo clean
$ cargo build
$ cargo test
```

## Examples

See the full example in the `examples/tagged` directory, which creates a RocksDB
database, adds a few records, establishes a secondary index, and queries that
index using specific key values.

### Quick Example

This code snippet is lifted from the aforementioned example. In shows the most
basic usage of opening the database, adding records, and querying an index.
Examples of functions for generating the index are in `examples/tagged`
directory.

```rust
let db_path = "my_database";
let mut views: Vec<String> = Vec::new();
views.push("tags".to_owned());
let dbase = Database::new(Path::new(db_path), views, Box::new(mapper)).unwrap();
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

// querying by a specific tag: cat
let result = dbase.query_by_key("tags", b"cat");
let iter = result.unwrap();
let results: Vec<QueryResult> = iter.collect();
for result in results {
    let doc_id = str::from_utf8(&result.doc_id).unwrap().to_owned();
    println!("query result key: {:}", doc_id);
}
```

## Design

### Usage

The application uses the `mokuroku::Database` struct rather than `rocksb::DB`,
as this crate will create and manage that `DB` instance. Since the crate is
managing the secondary indices, it is necessary for the application to call the
`put()` function on `Database`, rather than calling directly to `DB`. For
operations that should not affect any index, the application is free to get a
direct reference to `DB` using the `Database.db()` function.

At startup, the application will create an instance of `Database` and provide
three pieces of information.

1. Path to the database files, just as with `DB::open()`
1. Collection of view (index) names that will be passed to `Document.map()`
1. A boxed function of type `mokuroku::ByteMapper`

The set of view names are those indices which the library will update every time
`Database.put()` is called. That is, the implementation of `Document` that is
passed to the `put()` call will have its `map()` invoked with each of the
provided view names. Not every `Document` will emit index key/value pairs for
every view. In fact, there is no requirement to emit anything, it is entirely
application dependent. Similarly, a single invocation of a mapper may emit
multiple values, which is demonstrated in the `tagged` example.

The `ByteMapper` is necessary when the library needs to rebuild an index. Since
the library will be reading every record in the default column family, it does
not know how to deserialize the records into the appropriate `Document`
implementation. For this reason, the application must provide this function to
recognize and deserialize records, and then emit index key/value pairs.

After setting up the database, the application may want to invoke `query()` on
the database for each named view. This will cause the library to build the
indices if they are missing, which will improve the response time of subsequent
calls to `query()`.

### Data Model

The application defines the database record format; this library does not put
any restrictions on the format of the keys or values. That is a big part of why
the usage is slightly more complicated. This is _not_ PouchDB and the documents
are _not_ all JSON formatted blobs of stuff.

The library maintains the secondary indices in separate column families, whose
names start with `mrview-` to avoid collision with any column family that
application have created. For instance, if the application creates a view named
"tags", then the library will create a column family named `mrview-tags` and
populate it with the values given to the `Emitter` passed to the implementations
of `Document.map()` and `ByteMapper` defined by the application.

The index keys output by the application need not be unique. The library will
append a unique suffix (a [ULID](https://github.com/ulid/spec) to be specific)
to ensure that no index entry will overwrite any other. The application can emit
an optional value for the index entry, while the library will inject the primary
key automatically before saving the index row to the database. The format of
that index value is entirely up to the application.
