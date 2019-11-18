//
// Copyright (c) 2019 Nathan Fiedler
//
use failure::Error;
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
    tags: Vec<String>,
}

// See the crate documentation for more information.
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
        // This document trait only cares about one index, but it could very
        // well respond to more than just one view name.
        if view == "tags" {
            for tag in &self.tags {
                emitter.emit(tag.as_bytes(), Some(&self.location.as_bytes()))?;
            }
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
    let views = vec!["tags".to_owned()];
    let mut dbase = Database::new(Path::new(db_path), views, Box::new(mapper)).unwrap();
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
        Asset {
            key: String::from("asset/blackdog"),
            location: String::from("taiwan"),
            tags: vec![
                String::from("dog"),
                String::from("black"),
                String::from("fur"),
            ],
        },
        Asset {
            key: String::from("asset/whitecatears"),
            location: String::from("hakone"),
            tags: vec![
                String::from("cat"),
                String::from("white"),
                String::from("ears"),
            ],
        },
        Asset {
            key: String::from("asset/whitecattail"),
            location: String::from("hakone"),
            tags: vec![
                String::from("cat"),
                String::from("white"),
                String::from("tail"),
            ],
        },
        Asset {
            key: String::from("asset/whitemouse"),
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
    let count: usize = dbase.query("tags").unwrap().count();
    println!("There are {:} tags in the index.", count);

    // querying by a specific tag: cat
    println!("querying for 'cat'...");
    let results = dbase.query_by_key("tags", b"cat").unwrap();
    for result in results {
        let doc_id = str::from_utf8(&result.doc_id).unwrap().to_owned();
        println!("query result key: {:}", doc_id);
    }

    // query for rows that have all matching tags
    println!("querying for rows that have 'cat' and 'white'...");
    let keys: Vec<&[u8]> = vec![b"cat", b"white"];
    let results = dbase.query_all_keys("tags", &keys).unwrap();
    for result in results.iter() {
        let doc_id = str::from_utf8(&result.doc_id).unwrap().to_owned();
        println!("query result key: {:}", doc_id);
    }

    println!("counting occurrences of each key...");
    let counts = dbase.count_all_keys("tags").unwrap();
    for (tag, count) in counts.iter() {
        let tag_str = str::from_utf8(tag).unwrap().to_owned();
        println!("{:}: {:}", tag_str, count);
    }
}
