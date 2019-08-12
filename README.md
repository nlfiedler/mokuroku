# mokuroku

## Overview

Like [CouchDB](https://couchdb.apache.org) and [PouchDB](https://pouchdb.com),
this [Rust](https://www.rust-lang.org) crate seeks to provide a secondary index
on top of the [RocksDB](https://rocksdb.org) key/value store. Building a simple
index in an ad hoc fashion seems tempting, but managing an index is not easy,
and doing it well requires substantial effort.

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
