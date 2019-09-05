//
// Copyright (c) 2019 Nathan Fiedler
//

//! Create an instance of `Database`, much like you would with `rocksdb::DB`.
//! Provide the path to the database files, the set of view names to maintain,
//! and a `ByteMapper` that will assist in building indices from existing data.
//! See the README and `examples` directory for examples.
//!
//! The secondary indices are built when `Database.query()` is called and the
//! corresponding column family is missing. Knowing this, an application may
//! want to open the database and subsequently call `query()` for every view.
//! This will cause the index to be built based on the existing data. If for
//! whatever reason the application deems it necessary to rebuild an index, that
//! can be accomplished by calling the `rebuild()` function.
//!
//! The `Document.map()` functions are like redux.js reducers. Every view name
//! will eventually be passed to every implementation of `Document`, and it is
//! up to the application to ignore views that are not relevant for the given
//! document type.
//!
//! The reason for having both the `Document` and the `ByteMapper` is that we
//! want to avoid any unnecessary deserialization when updating the secondary
//! index. When putting a `Document` in the database, there is no need to
//! deserialize it, so the `map()` will be called on it with every available
//! view. However, in the case of a view being created from existing data, the
//! library must deserialize every record in the default column family, and that
//! is where the `ByteMapper` comes in -- it will recognize the key and/or value
//! and perform the appropriate deserialization (likely using an implementation
//! of `Document`) and invoke the provided `Emitter` with index values.
//!
//! **N.B.** _Presently any delete or update that would affect an index is not
//! correctly accounted for. That will be addressed in a future release. In the
//! mean time, you can forcibly rebuild an index by calling `rebuild()` on the
//! `Database` instance._
//!
//! **N.B.** The index key emitted by the application is combined with the data
//! record primary key, separated by a single null byte. Using a separator is
//! necessary since the library does not know in advance how long either of
//! these keys is expected to be, and the index query depends heavily on using
//! the prefix iterator to speed up the search. If you need to change the
//! separator, use the `Database.separator()` function. If at some later time
//! you decide to change the separator, you will need to rebuild the indices.

use failure::{err_msg, Error};
use rocksdb::{ColumnFamily, DBIterator, IteratorMode, Options, DB};
use std::fmt;
use std::path::Path;

///
/// `Document` defines a few operations required for building the index. Any
/// data that should contribute to an index should be represented by a type that
/// implements this trait, and be stored in the database using the `Database`
/// wrapper method `put()`.
///
/// The primary reason for this trait is that when `put()` is called with an
/// instance of `Document`, there is no need to deserialize the value when
/// calling `map()`, since it is already in its natural format.
///
pub trait Document: Sized {
    ///
    /// Deserializes a sequence of bytes to return a value of this type. The key
    /// is provided in case it is required for proper deserialization.
    ///
    fn from_bytes(key: &[u8], value: &[u8]) -> Result<Self, Error>;
    ///
    /// Serializes this value into a sequence of bytes.
    ///
    fn to_bytes(&self) -> Result<Vec<u8>, Error>;
    ///
    /// Map a value to zero or more index key/value pairs, as passed to the
    /// given emitter instance.
    ///
    fn map(&self, view: &str, emitter: &Emitter) -> Result<(), Error>;
}

///
/// The application implements a function of this type and passes it to the
/// database constructor. It will be called with the key and value for every
/// record in the database and is expected to invoke the `Emitter` to generate
/// index values. This function is necessary for the building of an index where
/// none exists. The library will read every record in the default column
/// family, invoking this mapper with that data.
///
/// The application could, for instance, use information in the key to determine
/// how to deserialize the value into a `Document`, and then invoke the `map()`
/// function on that `Document`.
///
/// Arguments: database key, database value, view name, emitter
///
pub type ByteMapper = Box<dyn Fn(&[u8], &[u8], &str, &Emitter) -> Result<(), Error>>;

///
/// The `Emitter` receives index key/value pairs from the application.
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
}

impl<'a> Emitter<'a> {
    /// Construct an Emitter for processing the given key and view.
    fn new(db: &'a DB, key: &'a [u8], cf: &'a ColumnFamily, sep: &'a [u8]) -> Self {
        Self { db, key, cf, sep }
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
        // index value is either the given value or an empty slice
        let empty = vec![];
        let id_value: &[u8] = if let Some(value) = value.as_ref() {
            value.as_ref()
        } else {
            &empty
        };
        self.db.put_cf(*self.cf, &uniq_key, id_value)?;
        Ok(())
    }
}

///
/// An instance of the database for reading and writing records to disk. This
/// wrapper manages the secondary indices defined by the application.
///
/// The secondary indices, referred to as "views", will be stored in RocksDB
/// column families whose names are based on the names given in the constructor,
/// with a prefix of `mrview-` to avoid conflicting with any column families
/// managed by the application. The library will manage these column families as
/// needed when (re)building the indices.
///
/// In addition to the names of the views, the application must provide a
/// mapping function that works with key/value pairs as slices of bytes. This is
/// called when building an index by reading every record in the database, in
/// which the library does not have an inferred `Document` implementation to
/// deserialize the record.
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
    /// Return a reference to the RocksDB instance. This is an escape hatch in
    /// the event that you need to call a function that is not exposed via this
    /// wrapper. Beware that interfacing directly with RocksDB means that the
    /// index is not being updated with respect to those operations.
    ///
    pub fn db(&self) -> &DB {
        &self.db
    }

    ///
    /// Put the data record key/value pair into the database, ensuring all
    /// indices are updated, if they have been built.
    ///
    /// _N.B. If updating a document results in previous index values being
    /// outdated, the index will be out of sync. This will be addressed in a
    /// future release._
    ///
    pub fn put<D, K>(&self, key: K, value: &D) -> Result<(), Error>
    where
        D: Document,
        K: AsRef<[u8]>,
    {
        let bytes = value.to_bytes()?;
        self.db.put(key.as_ref(), bytes)?;
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
    /// _N.B. This does not yet update the secondary indices.  This will be
    /// addressed in a future release._
    ///
    pub fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<(), Error> {
        self.db.delete(key.as_ref())?;
        Ok(())
    }

    ///
    /// Query on the given index, returning all results. If the index has not
    /// yet been built, it will be built prior to returning any results.
    ///
    pub fn query(&self, view: &str) -> Result<QueryIterator, Error> {
        let cf = self.ensure_view_built(view)?;
        let iter = self.db.iterator_cf(cf, IteratorMode::Start)?;
        let qiter = QueryIterator::new(iter, &self.key_sep);
        Ok(qiter)
    }

    ///
    /// Query on the given index, returning those results whose key prefix
    /// matches the value given. Only those index keys with a matching prefix
    /// will be returned.
    ///
    /// As with `query()`, if the index has not yet been built, it will be.
    ///
    pub fn query_by_key<K: AsRef<[u8]>>(&self, view: &str, key: K) -> Result<QueryIterator, Error> {
        let cf = self.ensure_view_built(view)?;
        let iter = self.db.prefix_iterator_cf(cf, key.as_ref())?;
        let qiter = QueryIterator::new_prefix(iter, &self.key_sep, key.as_ref());
        Ok(qiter)
    }

    ///
    /// Build the named index, replacing the index if it already exists. To
    /// simply ensure that an index has been built, call `query()`, which will
    /// not delete the existing index.
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
/// `QueryResult` represents a single result from a query. The `key` is that
/// which was emitted by the application, and similarly the `value` is whatever
/// the application emitted along with the key. The `doc_id` is the primary key
/// of the data record.
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
/// `QueryIterator` returns the results from a database query as an instance of
/// `QueryResult`.
///
pub struct QueryIterator<'a> {
    /// Reference to Database for fetching records.
    dbiter: DBIterator<'a>,
    /// Key prefix to filter results, if any.
    prefix: Option<Vec<u8>>,
    /// Index key separator sequence.
    key_sep: &'a [u8],
}

impl<'a> QueryIterator<'a> {
    /// Construct a new QueryIterator from the DBIterator.
    fn new(dbiter: DBIterator<'a>, sep: &'a [u8]) -> Self {
        Self {
            dbiter,
            prefix: None,
            key_sep: sep,
        }
    }

    /// Construct a new QueryIterator that only returns results whose key
    /// matches the given prefix.
    fn new_prefix(dbiter: DBIterator<'a>, sep: &'a [u8], prefix: &[u8]) -> Self {
        Self {
            dbiter,
            prefix: Some(prefix.to_owned()),
            key_sep: sep,
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
        // Should use Knuth–Morris–Pratt string-searching algorithm to find the
        // position for multi-byte separators. It is vastly more complicated,
        // but it would be faster when done many times over.
        //
        for (idx, win) in key.windows(self.key_sep.len()).enumerate() {
            if win == self.key_sep {
                return idx;
            }
        }
        0
    }
}

impl<'a> Iterator for QueryIterator<'a> {
    type Item = QueryResult;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((key, value)) = self.dbiter.next() {
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
            return Some(QueryResult::new(
                short_key.into_boxed_slice(),
                value,
                doc_id.into_boxed_slice(),
            ));
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
        let mut views: Vec<String> = Vec::new();
        views.push("value".to_owned());
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
        let mut views: Vec<String> = Vec::new();
        views.push("tags".to_owned());
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
        let mut views: Vec<String> = Vec::new();
        views.push("tags".to_owned());
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
        let result = dbase.query("tags");
        assert!(result.is_ok());
        let iter = result.unwrap();
        let results: Vec<QueryResult> = iter.collect();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn empty_view_name() {
        let db_path = "tmp/test/empty_view_name";
        let _ = fs::remove_dir_all(db_path);
        let mut views: Vec<String> = Vec::new();
        // intentionally passing an empty view name
        views.push("".to_owned());
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
        let result = dbase.query("");
        assert!(result.is_ok());
        let iter = result.unwrap();
        let results: Vec<QueryResult> = iter.collect();
        assert_eq!(results.len(), 0);
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
        let result = dbase.query("");
        assert!(result.is_ok());
        let iter = result.unwrap();
        let results: Vec<QueryResult> = iter.collect();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn query_by_key() {
        let db_path = "tmp/test/query_by_key";
        let _ = fs::remove_dir_all(db_path);
        let mut views: Vec<String> = Vec::new();
        views.push("tags".to_owned());
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

        // querying all tags should return 12
        let result = dbase.query("tags");
        assert!(result.is_ok());
        let iter = result.unwrap();
        let results: Vec<QueryResult> = iter.collect();
        assert_eq!(results.len(), 12);

        // querying by a specific tag: noman
        let result = dbase.query_by_key("tags", b"noman");
        assert!(result.is_ok());
        let iter = result.unwrap();
        let results: Vec<QueryResult> = iter.collect();
        assert_eq!(results.len(), 0);

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
        let mut views: Vec<String> = Vec::new();
        views.push("tags".to_owned());
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
}
