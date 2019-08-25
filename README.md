# mokuroku

## Overview

Like [PouchDB](https://pouchdb.com), this [Rust](https://www.rust-lang.org)
crate is designed to provide a secondary index on top of the
[RocksDB](https://rocksdb.org) key/value store. Building a simple index in an ad
hoc fashion seems tempting, but managing an index is not easy, and doing it well
requires substantial effort.

The application provides the implementations of the `Document` trait to suit the
various types of data to be stored in the database, and this library will invoke
a mapping function provided by the application to produce index key/value pairs.

The design of this library is not unlike PouchDB, albeit with an API suitable
for the language. Unlike PouchDB, however, this library does not put any
constraints on the format of the database records. As a result, the API relies
on the application to provide the functions for deserializing records and
invoking the `emit()` function with index key/value pairs.

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
