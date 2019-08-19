//
// Copyright (c) 2019 Nathan Fiedler
//
use failure::Error;
use rocksdb::{DBIterator, IteratorMode, Options, DB};
use std::convert::TryInto;
use std::io::Write;
use std::path::Path;
use ulid::Ulid;

///
/// `Document` defines the operations required for building the secondary index.
/// Any data that should have an index should be represented by a type that
/// implements this trait, and be stored in the database using the `Database`
/// wrapper method `put()`.
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
    /// Return the desired name of the secondary index. Queries can then be
    /// run against the index using this name.
    ///
    fn view_name() -> String;
    ///
    /// Map a value to zero or more index key/value pairs, as passed to the
    /// given `emit` function (first argument is the key, second is value).
    ///
    fn map<P>(&self, emit: P)
    where
        P: Fn(&[u8], &[u8]) -> ();
}

///
/// An instance of the database for reading and writing records to disk. This
/// wrapper manages the secondary indices defined by the application.
///
pub struct Database {
    /// RocksDB instance.
    db: DB,
}

// Secondary indices are column families with our special prefix.
const VIEW_PREFIX: &str = "mrview-";
// Key suffix is a dash and 26 character ULID hex string.
const KEY_SUFFIX_LEN: usize = 27;
// Maximum size in bytes of a record key for application data.
const MAX_KEY_LEN: usize = 4_294_967_296;
// Number of bytes used to represent the document key.
const SIZEOF_KEY: usize = 4;

impl Database {
    ///
    /// Create an instance of Database using the given path for storage.
    ///
    /// The set of document instances will be used to define the secondary
    /// indices; that is, their names will be used in naming the indices, and
    /// their `map()` functions will be used to generate the index rows via the
    /// provided `emit` function.
    ///
    pub fn new<I, N>(db_path: &Path, views: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = N>,
        N: Document,
    {
        let myviews: Vec<String> = views
            .into_iter()
            .map(|_| {
                let mut s = String::from(VIEW_PREFIX);
                s.push_str(&N::view_name());
                s
            })
            .collect();
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let db = DB::open_cf(&db_opts, db_path, myviews)?;
        Ok(Self { db })
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
        let mut view_name = String::from(VIEW_PREFIX);
        view_name.push_str(&D::view_name());
        let cf = self.db.cf_handle(&view_name).unwrap();
        let emit = |ikey: &[u8], ivalue: &[u8]| {
            let ulid = Ulid::new().to_string();
            // to allow for duplicate keys emitted from the map function, add a
            // unique suffix to the index key
            let mut uniq_key: Vec<u8> = Vec::with_capacity(ikey.len() + KEY_SUFFIX_LEN);
            uniq_key.extend_from_slice(&ikey[..]);
            uniq_key.push(b'-');
            uniq_key.extend_from_slice(&ulid.as_bytes());
            // index value is the original document key and the given value,
            // plus the length encoding for each (so we can separate them later)
            let mut id_value: Vec<u8> = Vec::with_capacity(key.len() + ivalue.len() + SIZEOF_KEY);
            let _ = id_value.write((key.len() as u32).to_le_bytes().as_ref());
            let _ = id_value.write(key);
            let _ = id_value.write(ivalue);
            let _ = self.db.put_cf(cf, &uniq_key, &id_value);
        };
        D::map(&value, emit);
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
    /// Query on the given index, returning all results.
    ///
    pub fn query(&self, view: &str) -> Result<QueryIterator, Error> {
        let mut view_name = String::from(VIEW_PREFIX);
        view_name.push_str(view);
        let cf = self.db.cf_handle(&view_name).unwrap();
        let iter = self.db.iterator_cf(cf, IteratorMode::Start)?;
        let qiter = QueryIterator::new(iter);
        Ok(qiter)
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
}

impl<'a> QueryIterator<'a> {
    /// Construct a new QueryIterator from the DBIterator.
    fn new(dbiter: DBIterator<'a>) -> Self {
        Self { dbiter }
    }
}

impl<'a> Iterator for QueryIterator<'a> {
    type Item = QueryResult;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((key, value)) = self.dbiter.next() {
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
        fn view_name() -> String {
            String::from("value")
        }
        fn map<P>(&self, _emit: P)
        where
            P: Fn(&[u8], &[u8]) -> (),
        {
        }
    }

    #[test]
    fn put_get_delete() {
        let db_path = "tmp/test/put_get_delete";
        let _ = fs::remove_dir_all(db_path);
        let mut views: Vec<LenVal> = Vec::new();
        views.push(LenVal {
            key: String::new(),
            len: 0,
            val: String::new(),
        });
        let dbase = Database::new(Path::new(db_path), views).unwrap();
        let document = LenVal {
            key: String::from("cafebabe"),
            len: 10,
            val: String::from("have a cup o' joe"),
        };
        let key = document.key.as_bytes();
        let result = dbase.put(&key, &document);
        assert!(result.is_ok());
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
        fn view_name() -> String {
            String::from("tags")
        }
        fn map<P>(&self, emit: P)
        where
            P: Fn(&[u8], &[u8]) -> (),
        {
            for tag in &self.tags {
                emit(tag.as_bytes(), &self.location.as_bytes());
            }
        }
    }

    #[test]
    fn asset_view_creation() {
        let db_path = "tmp/test/asset_view_creation";
        let _ = fs::remove_dir_all(db_path);
        let mut views: Vec<Asset> = Vec::new();
        views.push(Asset {
            key: String::new(),
            location: String::new(),
            tags: Vec::new(),
        });
        let dbase = Database::new(Path::new(db_path), views).unwrap();
        let document = Asset {
            key: String::from("cafebabe"),
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
        // ensure the document "id", index keys, and value are correct
        assert!(results.iter().all(|r| r.doc_id.as_ref() == b"cafebabe"));
        assert!(results.iter().all(|r| r.value.as_ref() == b"hawaii"));
        let tags: Vec<String> = results
            .iter()
            .map(|r| str::from_utf8(&r.key).unwrap().to_owned())
            .collect();
        assert!(tags.contains(&String::from("cat")));
        assert!(tags.contains(&String::from("black")));
        assert!(tags.contains(&String::from("tail")));
    }
}
