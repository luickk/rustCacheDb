extern crate rustcachedb;

use std::thread;
use std::time;

#[derive(Clone, Default, Debug, PartialEq)]
struct CacheString(String);

impl rustcachedb::GenericKeyVal<CacheString> for CacheString {
    fn get_size(self: &Self) -> Result<u16, rustcachedb::CacheDbError> {
        match self.0.chars().count().try_into() {
            Ok(size) => {
                return Ok(size);
            }
            Err(_) => {
                return Err(rustcachedb::CacheDbError::ProtocolSizeBufferOverflow);
            }
        }
    }

    // must clone string since into_bytes() is not implemented for
    // the string reference (at least not the copy trait which is required)
    fn get_bytes(self: &Self) -> Vec<u8> {
        let str_bytes = self.clone();
        str_bytes.0.into_bytes()
    }

    fn from_bytes(data: &[u8]) -> Result<Self, rustcachedb::CacheDbError> {
        if let Ok(data_str) = std::str::from_utf8(data) {
            return Ok(Self(data_str.to_string()));
        }
        Err(rustcachedb::CacheDbError::DecodingErr)
    }
}

fn extended_client_test() {
    let mut cache_client =
        rustcachedb::CacheClient::<CacheString, CacheString>::create_connect([127, 0, 0, 1], 8081)
            .unwrap();
    for i in 1..500 {
        let co = rustcachedb::KeyValObj {
            key: CacheString(String::from("brian")),
            val: CacheString(String::from(format!("itest{}", i))),
        };
        cache_client.push(co).unwrap();
    }
}

#[test]
fn extended_server_test() {
    let cache = rustcachedb::CacheDb::<CacheString, CacheString>::new([127, 0, 0, 1], 8081);
    let test_server_instance = thread::spawn(move || (cache.cache_db_server()));
    thread::sleep(time::Duration::from_secs(1));
    extended_client_test();
    if let Err(e) = test_server_instance.join() {
        panic!("{:?}", e);
    }
}
