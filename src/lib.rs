//
// Copyright (c) 2019 Nathan Fiedler
//

//! The secondary indices are built when `Database.query()` is called and the
//! corresponding column family is missing. An application may then want to open
//! the database and immediately call `query()` for every view. This will cause
//! the index to be built based on the existing data. If for whatever reason the
//! application deems it necessary to rebuild an index, that can be accomplished
//! by calling the `rebuild()` function.
//!
//! The `Document.map()` functions are like redux.js reducers. Every view will
//! eventually be passed to every implementation of `Document`, and it is up to
//! the application to ignore views that are not relevant for the given document
//! type.
//!
//! The reason for the `Document` and the `ByteMapper` is that we want to avoid
//! any unnecessary deserialization when updating the secondary index. When
//! putting a document in the database, there is no need to deserialize it, so
//! the `map()` will be called on the `Document` instance with every available
//! view. However, in the case of a view being created from existing data, we
//! must deserialize every record in the database, and that is where the
//! `ByteMapper` comes in -- it will recognize the key and/or value and perform
//! the appropriate deserialization (likely using an implementation of
//! `Document`) and invoke the provided `Emitter.emit()` with index values.

use failure::{err_msg, Error};
use rocksdb::{ColumnFamily, DBIterator, IteratorMode, Options, DB};
use std::convert::TryInto;
use std::fmt;
use std::io::Write;
use std::path::Path;
use ulid::Ulid;

///
/// `Document` defines a few operations required for building the index. Any
/// data that should contribute to an index should be represented by a type that
/// implements this trait, and be stored in the database using the `Database`
/// wrapper method `put()`.
///
/// The primary reason for this trait is that when `put()` is called with an
/// instance of `Document`, there is no need to deserialize the value when
/// calling `map()`, since it is already in the natural format.
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
/// record in the database and is expected to invoke the `emit` function to
/// generate index values. This function is necessary for the building of an
/// index where none exists. The library will read every record in the default
/// column family, invoking this mapper with that data.
///
/// The application could, for instance, use information in the key to determine
/// how to deserialize the value, and then invoke the `map()` function on the
/// appropriate `Document` implementation.
///
/// Arguments: key, value, view name, emitter
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
}

impl<'a> Emitter<'a> {
    /// Construct an Emitter for processing the given key and view.
    fn new(db: &'a DB, key: &'a [u8], cf: &'a ColumnFamily) -> Self {
        Self { db, key, cf }
    }

    ///
    /// Call this with an index key and value. The index key is _not_ required
    /// to be unique, and the optional value can be in any format.
    ///
    pub fn emit(&self, ikey: &[u8], ivalue: Option<&[u8]>) -> Result<(), Error> {
        let ulid = Ulid::new().to_string();
        // to allow for duplicate keys emitted from the map function, add a
        // unique suffix to the index key
        let mut uniq_key: Vec<u8> = Vec::with_capacity(ikey.len() + KEY_SUFFIX_LEN);
        uniq_key.extend_from_slice(&ikey[..]);
        uniq_key.push(b'-');
        uniq_key.extend_from_slice(&ulid.as_bytes());
        // index value is the original document key and the given value, plus
        // the length of the primary key so we can separate them later
        let value_len = ivalue.map_or(0, |v| v.len());
        let mut id_value: Vec<u8> = Vec::with_capacity(self.key.len() + value_len + SIZEOF_KEY);
        let _ = id_value.write((self.key.len() as u32).to_le_bytes().as_ref());
        let _ = id_value.write(self.key);
        if let Some(value) = ivalue {
            let _ = id_value.write(value);
        }
        self.db.put_cf(*self.cf, &uniq_key, &id_value)?;
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
}

// Secondary indices are column families with our special prefix. This prefix
// resembles that used by PouchDB for its indices, but it also happens to be an
// abbreviation of "mokuroku view", so it's all good.
const VIEW_PREFIX: &str = "mrview-";
// Key suffix is a dash and 26 character ULID hex string.
const KEY_SUFFIX_LEN: usize = 27;
// Maximum size in bytes of a record key for application data.
const MAX_KEY_LEN: usize = 4_294_967_296;
// Number of bytes used to represent the length of the document key.
const SIZEOF_KEY: usize = 4;

impl Database {
    ///
    /// Create an instance of Database using the given path for storage.
    ///
    /// The set of view names are passed to the `Document.map()` whenever a
    /// document is put into the database.
    ///
    /// The `ByteMapper` is responsible for deserializing any type of record and
    /// emitting index key/value pairs appropriately.
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
        })
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
    /// Put the key/value pair into the database.
    ///
    pub fn put<D: Document>(&self, key: &[u8], value: &D) -> Result<(), Error> {
        assert!(key.len() <= MAX_KEY_LEN, "key length must be under 4mb");
        let bytes = value.to_bytes()?;
        self.db.put(key, bytes)?;
        // process every index on update
        for view in &self.views {
            let mut mrview = String::from(VIEW_PREFIX);
            mrview.push_str(&view);
            // only update the index if the column family exists
            if let Some(cf) = self.db.cf_handle(&mrview) {
                let emitter = Emitter::new(&self.db, key, &cf);
                D::map(&value, &view, &emitter)?;
            }
        }
        Ok(())
    }

    ///
    /// Retrieve the value with the given key.
    ///
    pub fn get<D: Document>(&self, key: &[u8]) -> Result<Option<D>, Error> {
        let result = self.db.get(key)?;
        match result {
            Some(v) => Ok(Some(D::from_bytes(key, &v)?)),
            None => Ok(None),
        }
    }

    ///
    /// Delete the database record associated with the given key.
    ///
    pub fn delete(&self, key: &[u8]) -> Result<(), Error> {
        self.db.delete(key)?;
        Ok(())
    }

    ///
    /// Query on the given index, returning all results. If the index has not
    /// yet been built, it will be built prior to returning any results.
    ///
    pub fn query(&self, view: &str) -> Result<QueryIterator, Error> {
        let mut mrview = String::from(VIEW_PREFIX);
        mrview.push_str(view);
        // lazily build the index when it is queried
        if self.db.cf_handle(&mrview).is_none() {
            self.build(view, &mrview)?;
        }
        let cf = self
            .db
            .cf_handle(&mrview)
            .ok_or_else(|| err_msg("missing column familiy"))?;
        let iter = self.db.iterator_cf(cf, IteratorMode::Start)?;
        let qiter = QueryIterator::new(iter);
        Ok(qiter)
    }

    pub fn query_by_key(&self, view: &str, key: &[u8]) -> Result<QueryIterator, Error> {
        let mut mrview = String::from(VIEW_PREFIX);
        mrview.push_str(view);
        // lazily build the index when it is queried
        if self.db.cf_handle(&mrview).is_none() {
            self.build(view, &mrview)?;
        }
        let cf = self
            .db
            .cf_handle(&mrview)
            .ok_or_else(|| err_msg("missing column familiy"))?;
        let iter = self.db.prefix_iterator_cf(cf, key)?;
        let qiter = QueryIterator::new_prefix(iter, key);
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
    /// Build the named index now.
    ///
    fn build(&self, view: &str, mrview: &str) -> Result<(), Error> {
        let opts = Options::default();
        let cf = self.db.create_cf(&mrview, &opts)?;
        let iter = self.db.iterator(IteratorMode::Start);
        for (key, value) in iter {
            let emitter = Emitter::new(&self.db, &key, &cf);
            (*self.mapper)(&key, &value, view, &emitter)?;
        }
        Ok(())
    }
}

///
/// `QueryResult` represents a single result from a query.
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
}

impl<'a> QueryIterator<'a> {
    /// Construct a new QueryIterator from the DBIterator.
    fn new(dbiter: DBIterator<'a>) -> Self {
        Self {
            dbiter,
            prefix: None,
        }
    }

    /// Construct a new QueryIterator that only returns results whose key
    /// matches the given prefix.
    fn new_prefix(dbiter: DBIterator<'a>, prefix: &[u8]) -> Self {
        Self {
            dbiter,
            prefix: Some(prefix.to_owned()),
        }
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
                let pre_key = &key[..prefix.len()];
                if pre_key != &prefix[..] {
                    return None;
                }
            }
            // remove the dash and ULID suffix from the index key
            let end = key.len() - KEY_SUFFIX_LEN;
            let mut short_key: Vec<u8> = Vec::with_capacity(end);
            short_key.extend_from_slice(&key[..end]);
            // split the index value into the original document key and the
            // index value provided by the application
            let key_len = read_le_u32(&value) as usize;
            let mut doc_id: Vec<u8> = Vec::with_capacity(key_len);
            let key_end = key_len + SIZEOF_KEY;
            doc_id.extend_from_slice(&value[SIZEOF_KEY..key_end]);
            let mut ivalue: Vec<u8> = Vec::with_capacity(value.len() - key_end);
            ivalue.extend_from_slice(&value[key_end..]);
            return Some(QueryResult::new(
                short_key.into_boxed_slice(),
                ivalue.into_boxed_slice(),
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

/// Read a u32 from the first four bytes of the slice.
fn read_le_u32(input: &[u8]) -> u32 {
    let (int_bytes, _rest) = input.split_at(std::mem::size_of::<u32>());
    u32::from_le_bytes(int_bytes.try_into().unwrap())
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

        let result = dbase.get::<LenVal>(&key);
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_some());
        let actual = option.unwrap();
        assert_eq!(document.key, actual.key);
        assert_eq!(document.len, actual.len);
        assert_eq!(document.val, actual.val);
        let result = dbase.delete(&key);
        assert!(result.is_ok());
        let result = dbase.get::<LenVal>(&key);
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
}
