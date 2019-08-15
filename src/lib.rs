//
// Copyright (c) 2019 Nathan Fiedler
//
use failure::Error;
use rocksdb::DB;
use std::path::Path;

pub trait Document: Sized {
    /// Deserializes a sequence of bytes to return a value of this type. The key
    /// is provided in case it is required for proper deserialization.
    fn from_bytes(key: &[u8], value: &[u8]) -> Result<Self, Error>;

    /// Serializes this value into a sequence of bytes.
    fn to_bytes(&self) -> Result<Vec<u8>, Error>;
}

///
/// An instance of the database for reading and writing records to disk.
///
pub struct Database {
    /// RocksDB instance.
    db: DB,
}

impl Database {
    ///
    /// Create an instance of Database using the given path for storage
    ///
    pub fn new(db_path: &Path) -> Result<Self, Error> {
        let db = DB::open_default(db_path)?;
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
        let bytes = value.to_bytes()?;
        self.db.put(key, bytes)?;
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
    }

    #[test]
    fn get_put() {
        let db_path = "tmp/test/database";
        let _ = fs::remove_dir_all(db_path);
        let dbase = Database::new(Path::new(db_path)).unwrap();
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
    }
}
