//
// Copyright (c) 2020 Nathan Fiedler
//
use chrono::prelude::*;
use mokuroku::*;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::str;

/// Simplistic example of a "document".
#[derive(Serialize, Deserialize)]
struct Asset {
    #[serde(skip)]
    key: String,
    location: String,
    updated: DateTime<Utc>,
}

// See the crate documentation for more information.
impl Document for Asset {
    fn from_bytes(key: &[u8], value: &[u8]) -> Result<Self, Error> {
        let mut serde_result: Asset =
            serde_cbor::from_slice(value).map_err(|err| Error::Serde(format!("{}", err)))?;
        serde_result.key = str::from_utf8(key)?.to_owned();
        Ok(serde_result)
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let encoded: Vec<u8> =
            serde_cbor::to_vec(self).map_err(|err| Error::Serde(format!("{}", err)))?;
        Ok(encoded)
    }

    fn map(&self, view: &str, emitter: &Emitter) -> Result<(), Error> {
        // This document trait only cares about one index, but it could very
        // well respond to more than just one view name.
        if view == "updates" {
            let millis = self.updated.timestamp_millis();
            let bytes = millis.to_be_bytes().to_vec();
            let encoded = base32::encode(&bytes);
            emitter.emit(&encoded, None)?;
        }
        Ok(())
    }
}

/// An example of `ByteMapper` that recognizes database values based on the key
/// prefix. It uses the defined document trait to perform the deserialization,
/// and then invokes its `map()` to emit index key/value pairs.
fn mapper(key: &[u8], value: &[u8], view: &str, emitter: &Emitter) -> Result<(), Error> {
    if &key[..6] == b"asset/".as_ref() {
        let doc = Asset::from_bytes(key, value)?;
        doc.map(view, emitter)?;
    }
    Ok(())
}

fn main() {
    // set up the database and populate it with stuff
    let db_path = "tmp/assets/database";
    let _ = fs::remove_dir_all(db_path);
    let views = vec!["updates".to_owned()];
    let mut dbase = Database::open_default(Path::new(db_path), views, Box::new(mapper)).unwrap();
    let documents = [
        Asset {
            key: String::from("asset/july8"),
            location: String::from("hawaii"),
            updated: Utc.ymd(2014, 7, 8).and_hms(9, 10, 11),
        },
        Asset {
            key: String::from("asset/june9"),
            location: String::from("taiwan"),
            updated: Utc.ymd(2014, 6, 9).and_hms(9, 10, 11),
        },
        Asset {
            key: String::from("asset/october14"),
            location: String::from("hakone"),
            updated: Utc.ymd(2014, 10, 14).and_hms(9, 10, 11),
        },
        Asset {
            key: String::from("asset/august25"),
            location: String::from("hakone"),
            updated: Utc.ymd(2014, 8, 25).and_hms(9, 10, 11),
        },
        Asset {
            key: String::from("asset/may13"),
            location: String::from("dublin"),
            updated: Utc.ymd(2014, 5, 13).and_hms(9, 10, 11),
        },
        Asset {
            key: String::from("asset/september9"),
            location: String::from("dublin"),
            updated: Utc.ymd(2014, 9, 9).and_hms(9, 10, 11),
        },
        Asset {
            key: String::from("asset/january31"),
            location: String::from("oakland"),
            updated: Utc.ymd(2014, 1, 31).and_hms(9, 10, 11),
        },
        Asset {
            key: String::from("asset/april11"),
            location: String::from("oakland"),
            updated: Utc.ymd(2014, 4, 11).and_hms(9, 10, 11),
        },
    ];
    for document in documents.iter() {
        let key = document.key.as_bytes();
        let result = dbase.put(&key, document);
        assert!(result.is_ok());
    }

    // query for assets within a given range of time
    let millis = Utc.ymd(2014, 6, 21).and_hms(0, 0, 0).timestamp_millis();
    let bytes = millis.to_be_bytes().to_vec();
    let key_a = base32::encode(&bytes);
    let millis = Utc.ymd(2014, 9, 21).and_hms(0, 0, 0).timestamp_millis();
    let bytes = millis.to_be_bytes().to_vec();
    let key_b = base32::encode(&bytes);
    println!("querying for assets updated during summer...");
    let results = dbase.query_range("updates", &key_a, &key_b).unwrap();
    for result in results {
        let doc_id = str::from_utf8(&result.doc_id).unwrap().to_owned();
        println!("query result key: {:}", doc_id);
    }
}
