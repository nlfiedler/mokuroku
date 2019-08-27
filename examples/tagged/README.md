# mokuroku example: tagged

This example demonstrates the use of the `mokuroku` crate by populating a
database with records, and establishing a secondary index of "tags", then
querying the index based on tags.

## Requirements

* [Rust](https://www.rust-lang.org) stable (2018 edition)

## Running

Build and run the optimized version so the chunker performs a bit faster.

```shell
$ cargo run --release
```

The demo will create a database, populate it with some records, perform some
queries, and print the results to the console.
