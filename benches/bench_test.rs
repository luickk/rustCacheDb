#![feature(test)]

extern crate test;

use test::{Bencher};
use rustcachedb::{CacheDb, CacheClient, CacheDbError, KeyValObj};

#[derive(Clone, Default, Debug, PartialEq)]
struct CacheString(String);

impl rustcachedb::GenericKeyVal<CacheString> for CacheString {
    fn get_size(self: &Self) -> Result<u16, CacheDbError> {
        match self.0.chars().count().try_into() {
            Ok(size) => {
                return Ok(size);
            }
            Err(_) => {
                return Err(CacheDbError::ProtocolSizeBufferOverflow);
            }
        }
    }

    // must clone string since into_bytes() is not implemented for
    // the string reference (at least not the copy trait which is required)
    fn get_bytes(self: &Self) -> Vec<u8> {
        let str_bytes = self.clone();
        str_bytes.0.into_bytes()
    }

    fn from_bytes(data: &[u8]) -> Result<Self, CacheDbError> {
        if let Ok(data_str) = std::str::from_utf8(data) {
            return Ok(Self(data_str.to_string()));
        }
        Err(CacheDbError::DecodingErr)
    }
}

#[bench]
fn basic_client_pull_bench(b: &mut Bencher) {
    let cache = CacheDb::<CacheString, CacheString>::new([127, 0, 0, 1], 8080);
    CacheDb::<CacheString, CacheString>::cache_db_server(&cache);
    cache.push(KeyValObj{key: CacheString("asd".to_string()), val: CacheString("das".to_string())});
    
    let cache_client = CacheClient::<CacheString, CacheString>::create_connect([127, 0, 0, 1], 8080).unwrap();
    CacheClient::<CacheString, CacheString>::cache_client_handler(&cache_client);
    // .join().unwrap();

    let mut _test_res = KeyValObj{key: CacheString(String::new()), val: CacheString(String::new())};
    let pull_key = CacheString("asd".to_string());
    b.iter(|| {
        cache_client.pull(&pull_key, &mut _test_res).unwrap();   
    });
}
