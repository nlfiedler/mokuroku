//
// Copyright (c) 2019 Nathan Fiedler
//

//! ## Overview
//!
//! This crate will build and maintain a secondary index within RocksDB, similar
//! to what [PouchDB](https://pouchdb.com) does for LevelDB. The index keys and
//! optional values are provided by your application. Once an index has been
//! defined, queries for all entries, or those whose keys match a given prefix,
//! can be performed. The index is kept up-to-date as data records, the data
//! your application is storing in the database, are changed and/or deleted.
//!
//! ## Usage
//!
//! Create an instance of `Database` much like you would with `rocksdb::DB`.
//! Provide the path to the database files, the set of indices to maintain
//! (often referred to as _views_), and a `ByteMapper` that will assist in
//! building indices from existing data.
//!
//! Below is a very brief example. See the README and `examples` directory for
//! additional examples.
//!
//! ```no_run
//! # use failure::Error;
//! # use mokuroku::*;
//! # use std::path::Path;
//! fn mapper(key: &[u8], value: &[u8], view: &str, emitter: &Emitter) -> Result<(), Error> {
//!     // ... call emitter.emit() with index keys and optional values
//!     Ok(())
//! }
//! let db_path = "my_database";
//! let views = vec!["tags".to_owned()];
//! let dbase = Database::new(Path::new(db_path), views, Box::new(mapper)).unwrap();
//! ```
//!
//! ## Data Model
//!
//! The secondary indices are built when `Database.query()` is called and the
//! corresponding column family is missing. Knowing this, an application may
//! want to open the database and subsequently call `query()` for every view.
//! This will cause the index to be built based on the existing data. If for
//! whatever reason the application deems it necessary to rebuild an index, that
//! can be accomplished by calling the `rebuild()` function. When building an
//! index, the library will invoke the application's `ByteMapper` function.
//!
//! **N.B.** The index key emitted by the application is combined with the data
//! record primary key, separated by a single null byte. Using a separator is
//! necessary since the library does not know in advance how long either of
//! these keys is expected to be, and the index query depends heavily on using
//! the prefix iterator to speed up the search. If you want to specify a
//! different separator, use the `Database.separator()` function when opening
//! the database. If at some later time you decide to change the separator, you
//! will need to rebuild the indices.

use failure::{err_msg, Error};
use rocksdb::{ColumnFamily, DBIterator, IteratorMode, Options, DB};
use std::convert::TryInto;
use std::fmt;
use std::path::Path;
use std::time::SystemTime;

///
/// `Document` defines operations required for building the index.
///
/// Any data that should contribute to an index should be represented by a type
/// that implements this trait, and be stored in the database using the
/// `Database` wrapper method `put()`.
///
/// The primary reason for this trait is that when `put()` is called with an
/// instance of `Document`, there is no need to deserialize the value when
/// calling `map()`, since it is already in its natural form. The `map()`
/// function will eventually receive every view name that was initially provided
/// to `Database::new()`, and it is up to each `Document` implementation to
/// ignore views that are not relevant.
///
/// This example is using the [serde](https://crates.io/crates/serde) crate,
/// which provides the `Serialize` and `Deserialize` derivations.
///
/// ```no_run
/// # use failure::Error;
/// # use mokuroku::*;
/// # use serde::{Deserialize, Serialize};
/// # use std::str;
/// #[derive(Serialize, Deserialize)]
/// struct Asset {
///     #[serde(skip)]
///     key: String,
///     location: String,
///     tags: Vec<String>,
/// }
///
/// impl Document for Asset {
///     fn from_bytes(key: &[u8], value: &[u8]) -> Result<Self, Error> {
///         let mut serde_result: Asset = serde_cbor::from_slice(value)?;
///         serde_result.key = str::from_utf8(key)?.to_owned();
///         Ok(serde_result)
///     }
///
///     fn to_bytes(&self) -> Result<Vec<u8>, Error> {
///         let encoded: Vec<u8> = serde_cbor::to_vec(self)?;
///         Ok(encoded)
///     }
///
///     fn map(&self, view: &str, emitter: &Emitter) -> Result<(), Error> {
///         if view == "tags" {
///             for tag in &self.tags {
///                 emitter.emit(tag.as_bytes(), Some(&self.location.as_bytes()))?;
///             }
///         }
///         Ok(())
///     }
/// }
/// ```
///
pub trait Document: Sized {
    ///
    /// Deserializes a sequence of bytes to return a value of this type.
    ///
    /// The key is provided in case it is required for proper deserialization.
    /// For example, the document may be deserialized from the raw value, and
    /// then the unique identifier copied from the raw key.
    ///
    fn from_bytes(key: &[u8], value: &[u8]) -> Result<Self, Error>;
    ///
    /// Serializes this value into a sequence of bytes.
    ///
    fn to_bytes(&self) -> Result<Vec<u8>, Error>;
    ///
    /// Map this document to zero or more index key/value pairs.
    ///
    fn map(&self, view: &str, emitter: &Emitter) -> Result<(), Error>;
}

///
/// Responsible for emitting index key/value pairs for any given data record.
///
/// Arguments: data record key, data record value, view name, emitter
///
/// The application implements a function of this type and passes it to the
/// database constructor. When the library is building an index, this function
/// will be called with the key and value for every record in the default column
/// family. The mapping function is expected to invoke the `Emitter` to generate
/// the index key and value pairs. It is up to the mapper to recognize the key
/// and/or value of the data record, perform the appropriate deserialization,
/// and then invoke the provided `Emitter` with index values.
///
/// In this example, the `LenVal` and `Asset` types are implementations of
/// `Document`, making it a trivial matter to deserialize the database record
/// and emit index keys. This mapper uses the key prefix as a means of knowing
/// which `Document` implementation to use.
///
/// ```no_run
/// # use failure::Error;
/// # use mokuroku::*;
/// # struct Asset {}
/// # impl Document for Asset {
/// #     fn from_bytes(key: &[u8], value: &[u8]) -> Result<Self, Error> {
/// #         Ok(Asset {})
/// #     }
/// #     fn to_bytes(&self) -> Result<Vec<u8>, Error> {
/// #         Ok(Vec::new())
/// #     }
/// #     fn map(&self, view: &str, emitter: &Emitter) -> Result<(), Error> {
/// #         Ok(())
/// #     }
/// # }
/// # struct LenVal {}
/// # impl Document for LenVal {
/// #     fn from_bytes(key: &[u8], value: &[u8]) -> Result<Self, Error> {
/// #         Ok(LenVal {})
/// #     }
/// #     fn to_bytes(&self) -> Result<Vec<u8>, Error> {
/// #         Ok(Vec::new())
/// #     }
/// #     fn map(&self, view: &str, emitter: &Emitter) -> Result<(), Error> {
/// #         Ok(())
/// #     }
/// # }
/// fn mapper(key: &[u8], value: &[u8], view: &str, emitter: &Emitter) -> Result<(), Error> {
///     if &key[..3] == b"lv/".as_ref() {
///         let doc = LenVal::from_bytes(key, value)?;
///         doc.map(view, emitter)?;
///     } else if &key[..3] == b"as/".as_ref() {
///         let doc = Asset::from_bytes(key, value)?;
///         doc.map(view, emitter)?;
///     }
///     Ok(())
/// }
/// ```
///
pub type ByteMapper = Box<dyn Fn(&[u8], &[u8], &str, &Emitter) -> Result<(), Error>>;

///
/// The `Emitter` receives index key/value pairs from the application.
///
/// See the `Document` trait for an example.
///
pub struct Emitter<'a> {
    /// RocksDB reference
    db: &'a DB,
    /// Document primary key
    key: &'a [u8],
    /// Column family for the index
    cf: &'a ColumnFamily<'a>,
    /// Index key separator sequence.
    sep: &'a [u8],
    /// Duration since Unix epoch in nanos.
    ts: Vec<u8>,
}

impl<'a> Emitter<'a> {
    /// Construct an Emitter for processing the given key and view.
    fn new(db: &'a DB, key: &'a [u8], cf: &'a ColumnFamily, sep: &'a [u8]) -> Self {
        // get the current time for detecting stale records later; once
        // rust-rocksdb exposes the sequence number, use that instead
        let ts = nanos_since_epoch();
        Self {
            db,
            key,
            cf,
            sep,
            ts,
        }
    }

    ///
    /// Call this with an index key and value. Each data record can have zero or
    /// more index entries. The index key is _not_ required to be unique, and
    /// the optional value can be in any format.
    ///
    pub fn emit<B>(&self, key: B, value: Option<B>) -> Result<(), Error>
    where
        B: AsRef<[u8]>,
    {
        // to allow for duplicate keys emitted from the map function, add the
        // key separator and the primary data record key
        let len = key.as_ref().len() + self.sep.len() + self.key.len();
        let mut uniq_key: Vec<u8> = Vec::with_capacity(len);
        uniq_key.extend_from_slice(&key.as_ref()[..]);
        uniq_key.extend_from_slice(self.sep);
        uniq_key.extend_from_slice(self.key);
        // prefix the index value, if any, with the timestamp
        let value_len = value.as_ref().map_or(0, |v| v.as_ref().len());
        let mut ivalue: Vec<u8> = Vec::with_capacity(self.ts.len() + value_len);
        ivalue.extend_from_slice(&self.ts);
        if let Some(value) = value.as_ref() {
            ivalue.extend_from_slice(value.as_ref());
        }
        self.db.put_cf(*self.cf, &uniq_key, ivalue)?;
        Ok(())
    }
}

///
/// An instance of the database for reading and writing records to disk. This
/// wrapper manages the secondary indices defined by the application.
///
/// The secondary indices, often referred to as _views_, will be stored in
/// RocksDB column families whose names are based on the names given in the
/// constructor, with a prefix of `mrview-` to avoid conflicting with any column
/// families managed by the application. The library will manage these column
/// families as needed when (re)building the indices.
///
/// In addition to the names of the views, the application must provide a
/// mapping function that works with key/value pairs as slices of bytes. This is
/// called when building an index by reading every record in the default column
/// family, in which the library does not have an inferred `Document`
/// implementation to deserialize the record.
///
pub struct Database {
    /// RocksDB instance.
    db: DB,
    /// Collection of index ("view") names.
    views: Vec<String>,
    /// Given a document key/value pair, emits index key/value pairs.
    mapper: ByteMapper,
    /// The index key separator sequence.
    key_sep: Vec<u8>,
}

// Secondary indices are column families with our special prefix. This prefix
// resembles that used by PouchDB for its indices, but it also happens to be an
// abbreviation of "mokuroku view", so it's all good.
const VIEW_PREFIX: &str = "mrview-";

// Name of the column family where we track changed data records. For now the
// key is the data record primary key, and the value is the 16 bytes
// representing the nanoseconds since the Unix epoch. Eventually it should be
// the RocksDB sequence number, which seems more suited for this purpose.
const CHANGES_CF: &str = "mrview--changes";

impl Database {
    ///
    /// Create an instance of Database using the given path for storage.
    ///
    /// The set of view names are passed to the `Document.map()` whenever a
    /// document is put into the database. That is, these are the names of the
    /// indices that will be updated whenever a document is stored.
    ///
    /// The `ByteMapper` is responsible for deserializing any type of record and
    /// emitting index key/value pairs appropriately. It will be invoked when
    /// (re)building an index from existing data.
    ///
    pub fn new<I, N>(db_path: &Path, views: I, mapper: ByteMapper) -> Result<Self, Error>
    where
        I: IntoIterator<Item = N>,
        N: ToString,
    {
        let myviews: Vec<String> = views.into_iter().map(|ts| N::to_string(&ts)).collect();
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        // Do not create the missing column families now, otherwise it becomes
        // difficult to know if we need to build the indices.
        let db = DB::open(&db_opts, db_path)?;
        Ok(Self {
            db,
            views: myviews,
            mapper,
            key_sep: vec![0],
        })
    }

    ///
    /// Set the separator byte sequence used to separate the index key from the
    /// primary data record key in the index. The default is a single null byte.
    ///
    pub fn separator(mut self, sep: &[u8]) -> Self {
        if sep.is_empty() {
            panic!("separator must be one or more bytes");
        }
        self.key_sep = sep.to_owned();
        self
    }

    ///
    /// Return a reference to the RocksDB instance.
    ///
    /// This is an escape hatch in the event that you need to call a function
    /// that is not exposed via this wrapper. Beware that interfacing directly
    /// with RocksDB means that the index is not being updated with respect to
    /// those operations.
    ///
    pub fn db(&self) -> &DB {
        &self.db
    }

    ///
    /// Put the data record key/value pair into the database, ensuring all
    /// indices are updated, if they have been built.
    ///
    pub fn put<D, K>(&self, key: K, value: &D) -> Result<(), Error>
    where
        D: Document,
        K: AsRef<[u8]>,
    {
        let bytes = value.to_bytes()?;
        self.db.put(key.as_ref(), bytes)?;
        self.update_changes(key.as_ref())?;
        // process every index on update
        for view in &self.views {
            let mut mrview = String::from(VIEW_PREFIX);
            mrview.push_str(&view);
            // only update the index if the column family exists
            if let Some(cf) = self.db.cf_handle(&mrview) {
                let emitter = Emitter::new(&self.db, key.as_ref(), &cf, &self.key_sep);
                D::map(&value, &view, &emitter)?;
            }
        }
        Ok(())
    }

    ///
    /// Retrieve the data record with the given key.
    ///
    pub fn get<D, K>(&self, key: K) -> Result<Option<D>, Error>
    where
        D: Document,
        K: AsRef<[u8]>,
    {
        let result = self.db.get(key.as_ref())?;
        match result {
            Some(v) => Ok(Some(D::from_bytes(key.as_ref(), &v)?)),
            None => Ok(None),
        }
    }

    ///
    /// Delete the data record associated with the given key.
    ///
    pub fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<(), Error> {
        self.db.delete(key.as_ref())?;
        self.update_changes(key.as_ref())?;
        Ok(())
    }

    ///
    /// Insert a record of the change to the given primary key, tracking
    /// the time when this change was made.
    ///
    fn update_changes<K>(&self, key: K) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        let cf_opt = self.db.cf_handle(CHANGES_CF);
        let cf = if cf_opt.is_none() {
            let opts = Options::default();
            self.db.create_cf(CHANGES_CF, &opts)?
        } else {
            cf_opt.unwrap()
        };
        // currently using a timestamp, but would rather use a sequence number,
        // once rust-rocksdb exposes that function
        let ts = nanos_since_epoch();
        self.db.put_cf(cf, key.as_ref(), &ts)?;
        Ok(())
    }

    ///
    /// Query on the given index, returning all results.
    ///
    /// If the index has not yet been built, it will be built prior to returning
    /// any results.
    ///
    pub fn query(&self, view: &str) -> Result<QueryIterator, Error> {
        let cf = self.ensure_view_built(view)?;
        let iter = self.db.iterator_cf(cf, IteratorMode::Start)?;
        let qiter = QueryIterator::new(&self.db, iter, &self.key_sep, cf);
        Ok(qiter)
    }

    ///
    /// Query on the given index, returning those results whose key prefix
    /// matches the value given.
    ///
    /// Only those index keys with a matching prefix will be returned.
    ///
    /// As with `query()`, if the index has not yet been built, it will be.
    ///
    pub fn query_by_key<K: AsRef<[u8]>>(&self, view: &str, key: K) -> Result<QueryIterator, Error> {
        let cf = self.ensure_view_built(view)?;
        let iter = self.db.prefix_iterator_cf(cf, key.as_ref())?;
        let qiter = QueryIterator::new_prefix(&self.db, iter, &self.key_sep, cf, key.as_ref());
        Ok(qiter)
    }

    ///
    /// Query on the given index, returning those results whose key matches
    /// exactly the value given.
    ///
    /// Only those index entries with matching keys will be returned.
    ///
    /// As with `query()`, if the index has not yet been built, it will be.
    ///
    pub fn query_exact<K: AsRef<[u8]>>(&self, view: &str, key: K) -> Result<QueryIterator, Error> {
        let cf = self.ensure_view_built(view)?;
        let len = key.as_ref().len() + self.key_sep.len();
        let mut sep_key: Vec<u8> = Vec::with_capacity(len);
        sep_key.extend_from_slice(&key.as_ref()[..]);
        sep_key.extend_from_slice(&self.key_sep);
        let iter = self.db.prefix_iterator_cf(cf, &sep_key)?;
        let qiter = QueryIterator::new_prefix(&self.db, iter, &self.key_sep, cf, &sep_key);
        Ok(qiter)
    }

    ///
    /// Build the named index, replacing the index if it already exists.
    ///
    /// To simply ensure that an index has been built, call `query()`, which
    /// will not delete the existing index.
    ///
    pub fn rebuild(&self, view: &str) -> Result<(), Error> {
        let mut mrview = String::from(VIEW_PREFIX);
        mrview.push_str(view);
        if let Some(_cf) = self.db.cf_handle(&mrview) {
            self.db.drop_cf(&mrview)?;
        }
        self.build(view, &mrview)
    }

    ///
    /// Ensure the column family for the named view has been built.
    ///
    fn ensure_view_built(&self, view: &str) -> Result<ColumnFamily, Error> {
        let mut mrview = String::from(VIEW_PREFIX);
        mrview.push_str(view);
        // lazily build the index when it is queried
        if self.db.cf_handle(&mrview).is_none() {
            self.build(view, &mrview)?;
        }
        self.db
            .cf_handle(&mrview)
            .ok_or_else(|| err_msg("missing column family"))
    }

    ///
    /// Build the named index now.
    ///
    fn build(&self, view: &str, mrview: &str) -> Result<(), Error> {
        let opts = Options::default();
        let cf = self.db.create_cf(&mrview, &opts)?;
        let iter = self.db.iterator(IteratorMode::Start);
        for (key, value) in iter {
            let emitter = Emitter::new(&self.db, &key, &cf, &self.key_sep);
            (*self.mapper)(&key, &value, view, &emitter)?;
        }
        Ok(())
    }
}

///
/// Return a vector of 16 bytes representing the nanoseconds since the Unix
/// epoch, useful as a very precise timestamp.
///
fn nanos_since_epoch() -> Vec<u8> {
    let now = SystemTime::now();
    let ts: u128 = if let Ok(duration) = now.duration_since(std::time::UNIX_EPOCH) {
        duration.as_nanos()
    } else {
        0
    };
    let ts_bytes = ts.to_le_bytes();
    ts_bytes.to_vec()
}

///
/// Convert the first 16 bytes of the slice into a u128.
///
fn read_le_u128(input: &[u8]) -> u128 {
    let (int_bytes, _rest) = input.split_at(std::mem::size_of::<u128>());
    u128::from_le_bytes(int_bytes.try_into().unwrap())
}

///
/// Represents a single result from a query.
///
/// The `key` is that which was emitted by the application, and similarly the
/// `value` is whatever the application emitted along with the key (it will be
/// an empty vector if the application emitted `None`). The `doc_id` is the
/// primary key of the data record, a named borrowed from PouchDB.
///
#[derive(Debug)]
pub struct QueryResult {
    /// Secondary index key generated by the application.
    pub key: Box<[u8]>,
    /// Secondary index value generated by the application.
    pub value: Box<[u8]>,
    /// Document key from which this index entry was generated.
    pub doc_id: Box<[u8]>,
}

impl QueryResult {
    /// Construct a QueryResult based on index row values.
    fn new(key: Box<[u8]>, value: Box<[u8]>, doc_id: Box<[u8]>) -> Self {
        Self { key, value, doc_id }
    }
}

///
/// An `Iterator` returned by the database query functions, yielding instances
/// of `QueryResult` for each matching index entry.
///
/// ```no_run
/// # use failure::Error;
/// # use mokuroku::*;
/// # use std::path::Path;
/// # fn mapper(key: &[u8], value: &[u8], view: &str, emitter: &Emitter) -> Result<(), Error> {
/// #     Ok(())
/// # }
/// let db_path = "tmp/assets/database";
/// let views = vec!["tags".to_owned()];
/// let dbase = Database::new(Path::new(db_path), views, Box::new(mapper)).unwrap();
/// let result = dbase.query_by_key("tags", b"cat");
/// let iter = result.unwrap();
/// for result in iter {
///     let doc_id = std::str::from_utf8(&result.doc_id).unwrap().to_owned();
///     println!("query result key: {:}", doc_id);
/// }
/// ```
///
pub struct QueryIterator<'a> {
    /// Reference to Database for fetching records.
    db: &'a DB,
    /// Iterator for reading query results.
    dbiter: DBIterator<'a>,
    /// Key prefix to filter results, if any.
    prefix: Option<Vec<u8>>,
    /// Index key separator sequence.
    key_sep: &'a [u8],
    /// Column family for the index this query is based on.
    cf: ColumnFamily<'a>,
}

impl<'a> QueryIterator<'a> {
    /// Construct a new QueryIterator from the DBIterator.
    fn new(db: &'a DB, dbiter: DBIterator<'a>, sep: &'a [u8], cf: ColumnFamily<'a>) -> Self {
        Self {
            db,
            dbiter,
            prefix: None,
            key_sep: sep,
            cf,
        }
    }

    /// Construct a new QueryIterator that only returns results whose key
    /// matches the given prefix.
    fn new_prefix(
        db: &'a DB,
        dbiter: DBIterator<'a>,
        sep: &'a [u8],
        cf: ColumnFamily<'a>,
        prefix: &[u8],
    ) -> Self {
        Self {
            db,
            dbiter,
            prefix: Some(prefix.to_owned()),
            key_sep: sep,
            cf,
        }
    }

    /// Find the position of the key separator in the index key.
    fn find_separator(&self, key: &[u8]) -> usize {
        // for single-byte separators, use the simple approach
        if self.key_sep.len() == 1 {
            if let Some(pos) = key.iter().position(|&x| x == self.key_sep[0]) {
                return pos;
            }
            return 0;
        }
        //
        // Could use Knuth–Morris–Pratt string-searching algorithm to find the
        // position for multi-byte separators. However, it is complicated and
        // would pay off if the "text string" were long, but the composite keys
        // are fairly short, and the index keys are likely even shorter.
        //
        for (idx, win) in key.windows(self.key_sep.len()).enumerate() {
            if win == self.key_sep {
                return idx;
            }
        }
        0
    }

    /// Indicate if the given timestamp for an index entry is older than the
    /// "changes" entry for the given primary key (i.e. it is stale).
    fn is_stale(&self, key: &[u8], ts: &[u8]) -> bool {
        //
        // Caching: could use a bloom filter to avoid reading from the database
        // multiple times for the same primary key. However, that is likely to
        // be overkill considering the number of keys emitted for a single data
        // record is going to be low. Even just a simple Mutex<Option> of the
        // most recent stale primary key might be a waste of effort.
        //
        if let Some(cf) = self.db.cf_handle(CHANGES_CF) {
            if let Ok(Some(val)) = self.db.get_cf(cf, key) {
                let index_ts = read_le_u128(ts);
                let changed_ts = read_le_u128(&val);
                return index_ts < changed_ts;
            }
        }
        false
    }
}

impl<'a> Iterator for QueryIterator<'a> {
    type Item = QueryResult;

    fn next(&mut self) -> Option<Self::Item> {
        // loop until we find a non-stale entry, or run out entirely
        while let Some((key, value)) = self.dbiter.next() {
            if let Some(prefix) = &self.prefix {
                // enforce the primary key matching the given prefix
                if key.len() < prefix.len() {
                    return None;
                }
                if key[..prefix.len()] != prefix[..] {
                    return None;
                }
            }
            // separate the index key from the primary key
            let sep_pos = self.find_separator(&key);
            let short_key = key[..sep_pos].to_vec();
            let doc_id = key[sep_pos + self.key_sep.len()..].to_vec();
            let ts = value[..16].to_vec();
            if self.is_stale(&doc_id, &ts) {
                // prune the stale entry so we never see it again
                // n.b. the result is Ok even if the record never existed
                let _ = self.db.delete_cf(self.cf, key);
            } else {
                // lop off the 16 byte timestamp from the index value
                let ivalue = value[16..].to_vec();
                return Some(QueryResult::new(
                    short_key.into_boxed_slice(),
                    ivalue.into_boxed_slice(),
                    doc_id.into_boxed_slice(),
                ));
            }
        }
        None
    }
}

impl<'a> fmt::Debug for QueryIterator<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "QueryIterator")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::fs;
    use std::str;

    #[derive(Serialize, Deserialize)]
    struct LenVal {
        #[serde(skip)]
        key: String,
        len: usize,
        val: String,
    }

    impl Document for LenVal {
        fn from_bytes(key: &[u8], value: &[u8]) -> Result<Self, Error> {
            let mut serde_result: LenVal = serde_cbor::from_slice(value)?;
            serde_result.key = str::from_utf8(key)?.to_owned();
            Ok(serde_result)
        }

        fn to_bytes(&self) -> Result<Vec<u8>, Error> {
            let encoded: Vec<u8> = serde_cbor::to_vec(self)?;
            Ok(encoded)
        }

        fn map(&self, view: &str, emitter: &Emitter) -> Result<(), Error> {
            if view == "value" {
                emitter.emit(self.val.as_bytes(), None)?;
            }
            Ok(())
        }
    }

    #[test]
    fn put_get_delete() {
        let db_path = "tmp/test/put_get_delete";
        let _ = fs::remove_dir_all(db_path);
        let views = vec!["value".to_owned()];
        let dbase = Database::new(Path::new(db_path), views, Box::new(mapper)).unwrap();
        let document = LenVal {
            key: String::from("lv/deadbeef"),
            len: 12,
            val: String::from("deceased cow"),
        };
        let key = document.key.as_bytes();
        let result = dbase.put(&key, &document);
        assert!(result.is_ok());

        let result = dbase.query("value");
        assert!(result.is_ok());
        let iter = result.unwrap();
        let results: Vec<QueryResult> = iter.collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key.as_ref(), b"deceased cow");
        assert_eq!(results[0].doc_id.as_ref(), b"lv/deadbeef");
        assert_eq!(results[0].value.as_ref(), b"");

        let result: Result<Option<LenVal>, Error> = dbase.get(&key);
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_some());
        let actual = option.unwrap();
        assert_eq!(document.key, actual.key);
        assert_eq!(document.len, actual.len);
        assert_eq!(document.val, actual.val);
        let result = dbase.delete(&key);
        assert!(result.is_ok());
        let result: Result<Option<LenVal>, Error> = dbase.get(&key);
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_none());
        // repeated delete is ok and not an error
        let result = dbase.delete(&key);
        assert!(result.is_ok());
    }

    #[derive(Serialize, Deserialize)]
    struct Asset {
        #[serde(skip)]
        key: String,
        location: String,
        tags: Vec<String>,
    }

    impl Document for Asset {
        fn from_bytes(key: &[u8], value: &[u8]) -> Result<Self, Error> {
            let mut serde_result: Asset = serde_cbor::from_slice(value)?;
            serde_result.key = str::from_utf8(key)?.to_owned();
            Ok(serde_result)
        }

        fn to_bytes(&self) -> Result<Vec<u8>, Error> {
            let encoded: Vec<u8> = serde_cbor::to_vec(self)?;
            Ok(encoded)
        }

        fn map(&self, view: &str, emitter: &Emitter) -> Result<(), Error> {
            if view == "tags" {
                for tag in &self.tags {
                    emitter.emit(tag.as_bytes(), Some(&self.location.as_bytes()))?;
                }
            } else if view == "location" {
                emitter.emit(self.location.as_bytes(), None)?;
            }
            Ok(())
        }
    }

    fn mapper(key: &[u8], value: &[u8], view: &str, emitter: &Emitter) -> Result<(), Error> {
        if &key[..3] == b"lv/".as_ref() {
            let doc = LenVal::from_bytes(key, value)?;
            doc.map(view, emitter)?;
        } else if &key[..3] == b"as/".as_ref() {
            let doc = Asset::from_bytes(key, value)?;
            doc.map(view, emitter)?;
        }
        Ok(())
    }

    #[test]
    fn asset_view_creation() {
        let db_path = "tmp/test/asset_view_creation";
        let _ = fs::remove_dir_all(db_path);
        let views = vec!["tags".to_owned()];
        let dbase = Database::new(Path::new(db_path), views, Box::new(mapper)).unwrap();
        let document = Asset {
            key: String::from("as/cafebabe"),
            location: String::from("hawaii"),
            tags: vec![
                String::from("cat"),
                String::from("black"),
                String::from("tail"),
            ],
        };
        let key = document.key.as_bytes();
        let result = dbase.put(&key, &document);
        assert!(result.is_ok());
        let result = dbase.query("tags");
        assert!(result.is_ok());
        let iter = result.unwrap();
        let results: Vec<QueryResult> = iter.collect();
        assert_eq!(results.len(), 3);
        // ensure the document "id", index keys, and value are correct
        assert!(results.iter().all(|r| r.doc_id.as_ref() == b"as/cafebabe"));
        assert!(results.iter().all(|r| r.value.as_ref() == b"hawaii"));
        let tags: Vec<String> = results
            .iter()
            .map(|r| str::from_utf8(&r.key).unwrap().to_owned())
            .collect();
        assert_eq!(tags.len(), 3);
        assert!(tags.contains(&String::from("cat")));
        assert!(tags.contains(&String::from("black")));
        assert!(tags.contains(&String::from("tail")));
    }

    #[test]
    fn no_view_creation() {
        let db_path = "tmp/test/no_view_creation";
        let _ = fs::remove_dir_all(db_path);
        let views = vec!["tags".to_owned()];
        let dbase = Database::new(Path::new(db_path), views, Box::new(mapper)).unwrap();
        let document = Asset {
            // intentionally using a key that does not match anything in our mapper
            key: String::from("none/cafebabe"),
            location: String::from("hawaii"),
            tags: vec![
                String::from("cat"),
                String::from("black"),
                String::from("tail"),
            ],
        };
        let key = document.key.as_bytes();
        let result = dbase.put(&key, &document);
        assert!(result.is_ok());
        assert_eq!(count_index(&dbase, "tags"), 0);
    }

    #[test]
    fn empty_view_name() {
        let db_path = "tmp/test/empty_view_name";
        let _ = fs::remove_dir_all(db_path);
        // intentionally passing an empty view name
        let views = vec!["".to_owned()];
        let dbase = Database::new(Path::new(db_path), views, Box::new(mapper)).unwrap();
        let document = Asset {
            key: String::from("as/cafebabe"),
            location: String::from("hawaii"),
            tags: vec![
                String::from("cat"),
                String::from("black"),
                String::from("tail"),
            ],
        };
        let key = document.key.as_bytes();
        let result = dbase.put(&key, &document);
        assert!(result.is_ok());
        assert_eq!(count_index(&dbase, ""), 0);
    }

    #[test]
    fn empty_view_list() {
        let db_path = "tmp/test/empty_view_list";
        let _ = fs::remove_dir_all(db_path);
        let views: Vec<String> = Vec::new();
        // intentionally passing an empty view list
        let dbase = Database::new(Path::new(db_path), views, Box::new(mapper)).unwrap();
        let document = Asset {
            key: String::from("as/cafebabe"),
            location: String::from("hawaii"),
            tags: vec![
                String::from("cat"),
                String::from("black"),
                String::from("tail"),
            ],
        };
        let key = document.key.as_bytes();
        let result = dbase.put(&key, &document);
        assert!(result.is_ok());
        assert_eq!(count_index(&dbase, ""), 0);
    }

    #[test]
    fn query_by_key() {
        let db_path = "tmp/test/query_by_key";
        let _ = fs::remove_dir_all(db_path);
        let views = vec!["tags".to_owned()];
        let dbase = Database::new(Path::new(db_path), views, Box::new(mapper)).unwrap();
        let documents = [
            Asset {
                key: String::from("as/cafebabe"),
                location: String::from("hawaii"),
                tags: vec![
                    String::from("cat"),
                    String::from("black"),
                    String::from("tail"),
                ],
            },
            Asset {
                key: String::from("as/cafed00d"),
                location: String::from("taiwan"),
                tags: vec![
                    String::from("dog"),
                    String::from("black"),
                    String::from("fur"),
                ],
            },
            Asset {
                key: String::from("as/1badb002"),
                location: String::from("hakone"),
                tags: vec![
                    String::from("cat"),
                    String::from("white"),
                    String::from("ears"),
                ],
            },
            Asset {
                key: String::from("as/baadf00d"),
                location: String::from("dublin"),
                tags: vec![
                    String::from("mouse"),
                    String::from("white"),
                    String::from("tail"),
                ],
            },
        ];
        for document in documents.iter() {
            let key = document.key.as_bytes();
            let result = dbase.put(&key, document);
            assert!(result.is_ok());
        }

        assert_eq!(count_index(&dbase, "tags"), 12);
        assert_eq!(count_index_by_query(&dbase, "tags", b"noman"), 0);

        // querying by a specific tag: cat
        let result = dbase.query_by_key("tags", b"cat");
        assert!(result.is_ok());
        let iter = result.unwrap();
        let results: Vec<QueryResult> = iter.collect();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.key.as_ref() == b"cat"));
        let keys: Vec<String> = results
            .iter()
            .map(|r| str::from_utf8(&r.doc_id).unwrap().to_owned())
            .collect();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&String::from("as/cafebabe")));
        assert!(keys.contains(&String::from("as/1badb002")));

        // querying by a specific tag: white
        let result = dbase.query_by_key("tags", b"white");
        assert!(result.is_ok());
        let iter = result.unwrap();
        let results: Vec<QueryResult> = iter.collect();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.key.as_ref() == b"white"));
        let keys: Vec<String> = results
            .iter()
            .map(|r| str::from_utf8(&r.doc_id).unwrap().to_owned())
            .collect();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&String::from("as/baadf00d")));
        assert!(keys.contains(&String::from("as/1badb002")));
    }

    #[test]
    fn query_by_key_separator() {
        // query by key with a multi-byte key separator sequence
        let db_path = "tmp/test/query_by_key_separator";
        let _ = fs::remove_dir_all(db_path);
        let views = vec!["tags".to_owned()];
        let dbase = Database::new(Path::new(db_path), views, Box::new(mapper)).unwrap();
        let sep = vec![0xff, 0xff, 0xff];
        let dbase = dbase.separator(&sep);
        let documents = [
            Asset {
                key: String::from("as/cafebabe"),
                location: String::from("hawaii"),
                tags: vec![
                    String::from("cat"),
                    String::from("black"),
                    String::from("tail"),
                ],
            },
            Asset {
                key: String::from("as/cafed00d"),
                location: String::from("taiwan"),
                tags: vec![
                    String::from("dog"),
                    String::from("black"),
                    String::from("fur"),
                ],
            },
            Asset {
                key: String::from("as/1badb002"),
                location: String::from("hakone"),
                tags: vec![
                    String::from("cat"),
                    String::from("white"),
                    String::from("ears"),
                ],
            },
            Asset {
                key: String::from("as/baadf00d"),
                location: String::from("dublin"),
                tags: vec![
                    String::from("mouse"),
                    String::from("white"),
                    String::from("tail"),
                ],
            },
        ];
        for document in documents.iter() {
            let key = document.key.as_bytes();
            let result = dbase.put(&key, document);
            assert!(result.is_ok());
        }

        // querying by a specific tag: cat
        let result = dbase.query_by_key("tags", b"cat");
        assert!(result.is_ok());
        let iter = result.unwrap();
        let results: Vec<QueryResult> = iter.collect();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.key.as_ref() == b"cat"));
        let keys: Vec<String> = results
            .iter()
            .map(|r| str::from_utf8(&r.doc_id).unwrap().to_owned())
            .collect();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&String::from("as/cafebabe")));
        assert!(keys.contains(&String::from("as/1badb002")));
    }

    /// Query the database and return the number of results.
    fn count_index(dbase: &Database, view: &str) -> usize {
        let result = dbase.query(view);
        assert!(result.is_ok());
        let iter = result.unwrap();
        iter.count()
    }

    /// Return the number of records matching the query.
    fn count_index_by_query(dbase: &Database, view: &str, query: &[u8]) -> usize {
        let result = dbase.query_by_key(view, query);
        assert!(result.is_ok());
        let iter = result.unwrap();
        iter.count()
    }

    /// Return the number of records exactly matching the query.
    fn count_index_exact(dbase: &Database, view: &str, query: &[u8]) -> usize {
        let result = dbase.query_exact(view, query);
        assert!(result.is_ok());
        let iter = result.unwrap();
        iter.count()
    }

    /// Return the number of records in the named index without using the
    /// database query function, which performs compaction.
    fn count_index_records(db: &DB, view: &str) -> usize {
        let mut mrview = String::from(VIEW_PREFIX);
        mrview.push_str(view);
        let result = db
            .cf_handle(&mrview)
            .ok_or_else(|| err_msg("missing column family"));
        assert!(result.is_ok());
        let cf = result.unwrap();
        let result = db.iterator_cf(cf, IteratorMode::Start);
        assert!(result.is_ok());
        let iter = result.unwrap();
        iter.count()
    }

    #[test]
    fn query_with_changes() {
        let db_path = "tmp/test/query_with_changes";
        let _ = fs::remove_dir_all(db_path);
        let views = vec!["tags".to_owned()];
        let dbase = Database::new(Path::new(db_path), &views, Box::new(mapper)).unwrap();
        let documents = [
            Asset {
                key: String::from("as/cafebabe"),
                location: String::from("hawaii"),
                tags: vec![
                    String::from("cat"),
                    String::from("black"),
                    String::from("tail"),
                ],
            },
            Asset {
                key: String::from("as/cafed00d"),
                location: String::from("taiwan"),
                tags: vec![
                    String::from("dog"),
                    String::from("black"),
                    String::from("fur"),
                ],
            },
            Asset {
                key: String::from("as/1badb002"),
                location: String::from("hakone"),
                tags: vec![
                    String::from("cat"),
                    String::from("white"),
                    String::from("ears"),
                ],
            },
            Asset {
                key: String::from("as/baadf00d"),
                location: String::from("dublin"),
                tags: vec![
                    String::from("mouse"),
                    String::from("white"),
                    String::from("tail"),
                ],
            },
        ];
        for document in documents.iter() {
            let key = document.key.as_bytes();
            let result = dbase.put(&key, document);
            assert!(result.is_ok());
        }

        assert_eq!(count_index_by_query(&dbase, "tags", b"fur"), 1);
        assert_eq!(count_index_by_query(&dbase, "tags", b"tail"), 2);
        assert_eq!(count_index_by_query(&dbase, "tags", b"cat"), 2);
        assert_eq!(count_index_records(dbase.db(), "tags"), 12);

        // update a couple of existing records
        let documents = [
            Asset {
                key: String::from("as/cafed00d"),
                location: String::from("taiwan"),
                tags: vec![
                    String::from("dog"),
                    String::from("black"),
                    String::from("fuzz"),
                ],
            },
            Asset {
                key: String::from("as/1badb002"),
                location: String::from("hakone"),
                tags: vec![
                    String::from("dog"),
                    String::from("black"),
                    String::from("tail"),
                ],
            },
        ];
        for document in documents.iter() {
            let key = document.key.as_bytes();
            let result = dbase.put(&key, document);
            assert!(result.is_ok());
        }

        assert_eq!(count_index_records(dbase.db(), "tags"), 16);
        // this query should perform a compaction!
        assert_eq!(count_index(&dbase, "tags"), 12);
        assert_eq!(count_index_records(dbase.db(), "tags"), 12);
        assert_eq!(count_index_by_query(&dbase, "tags", b"cat"), 1);
        assert_eq!(count_index_by_query(&dbase, "tags", b"fur"), 0);
        assert_eq!(count_index_by_query(&dbase, "tags", b"fuzz"), 1);
        assert_eq!(count_index_by_query(&dbase, "tags", b"tail"), 3);

        // delete an entry, and query again to perform compaction
        let result = dbase.delete(String::from("as/baadf00d").as_bytes());
        assert!(result.is_ok());
        assert_eq!(count_index(&dbase, "tags"), 9);
        assert_eq!(count_index_records(dbase.db(), "tags"), 9);
        assert_eq!(count_index_by_query(&dbase, "tags", b"mouse"), 0);
    }

    #[test]
    fn query_exact() {
        let db_path = "tmp/test/query_exact";
        let _ = fs::remove_dir_all(db_path);
        let views = vec!["tags".to_owned()];
        let dbase = Database::new(Path::new(db_path), &views, Box::new(mapper)).unwrap();
        let documents = [
            Asset {
                key: String::from("as/onecat"),
                location: String::from("tree"),
                tags: vec![
                    String::from("cat"),
                    String::from("orange"),
                    String::from("tail"),
                ],
            },
            Asset {
                key: String::from("as/twocats"),
                location: String::from("backyard"),
                tags: vec![
                    String::from("cats"),
                    String::from("oranges"),
                    String::from("tails"),
                ],
            },
        ];
        for document in documents.iter() {
            let key = document.key.as_bytes();
            let result = dbase.put(&key, document);
            assert!(result.is_ok());
        }

        assert_eq!(count_index_by_query(&dbase, "tags", b"cat"), 2);
        assert_eq!(count_index_by_query(&dbase, "tags", b"cats"), 1);
        assert_eq!(count_index_by_query(&dbase, "tags", b"tail"), 2);
        assert_eq!(count_index_exact(&dbase, "tags", b"cat"), 1);
        assert_eq!(count_index_exact(&dbase, "tags", b"orange"), 1);
        assert_eq!(count_index_exact(&dbase, "tags", b"tail"), 1);
    }

    #[test]
    fn index_rebuild() {
        let db_path = "tmp/test/index_rebuild";
        let _ = fs::remove_dir_all(db_path);
        // only mention the one view, we will build the other manually
        let views = vec!["tags".to_owned()];
        let dbase = Database::new(Path::new(db_path), &views, Box::new(mapper)).unwrap();
        let documents = [
            Asset {
                key: String::from("as/blackcat"),
                location: String::from("hallows"),
                tags: vec![
                    String::from("cat"),
                    String::from("black"),
                    String::from("tail"),
                ],
            },
            Asset {
                key: String::from("as/blackdog"),
                location: String::from("moors"),
                tags: vec![
                    String::from("dog"),
                    String::from("black"),
                    String::from("fur"),
                ],
            },
            Asset {
                key: String::from("as/whitecat"),
                location: String::from("upstairs"),
                tags: vec![
                    String::from("cat"),
                    String::from("white"),
                    String::from("ears"),
                ],
            },
            Asset {
                key: String::from("as/whitemouse"),
                location: String::from("attic"),
                tags: vec![
                    String::from("mouse"),
                    String::from("white"),
                    String::from("tail"),
                ],
            },
        ];
        for document in documents.iter() {
            let key = document.key.as_bytes();
            let result = dbase.put(&key, document);
            assert!(result.is_ok());
        }

        assert_eq!(count_index_by_query(&dbase, "tags", b"fur"), 1);
        let result = dbase.rebuild("tags");
        assert!(result.is_ok());
        assert_eq!(count_index_by_query(&dbase, "tags", b"fur"), 1);
        let result = dbase.rebuild("location");
        assert!(result.is_ok());
        assert_eq!(count_index_by_query(&dbase, "location", b"attic"), 1);
    }
}
