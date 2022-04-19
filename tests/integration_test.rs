extern crate rustcachedb;

use std::thread;
use std::time;
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

fn extended_client_test() {
    let mut cache_client = CacheClient::<CacheString, CacheString>::create_connect([127, 0, 0, 1], 8081).unwrap();
    for i in 1..500 {
        let co = KeyValObj {
            key: CacheString(String::from(format!("key{}", i))),
            val: CacheString(String::from(format!("val{}", i))),
        };
        cache_client.push(co).unwrap();
    }

    let mut pull_k: CacheString;
    let mut pull_v: CacheString;
    for i in 1..500 {
        pull_k = CacheString(String::from(format!("key{}", i)));
        pull_v = CacheString(String::from(format!("val{}", i)));
        // let get_res = cache_client.pull(&pull_k).unwrap();
        // assert_eq!(&get_res.val, format!("val{}", i));
    }

    cache_client.terminate_conn().unwrap();
}

#[test]
fn extended_server_test() {
    let cache = CacheDb::<CacheString, CacheString>::new([127, 0, 0, 1], 8081);

    cache.write().unwrap().push(KeyValObj {
        key: CacheString(String::from("brian")),
        val: CacheString(String::from("test")),
    });

    let _test_server_instance = thread::spawn(move || (CacheDb::<CacheString, CacheString>::cache_db_server(cache)));

    thread::sleep(time::Duration::from_secs(1));
    extended_client_test();
}
