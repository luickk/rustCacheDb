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

    fn empty() -> CacheString {
        CacheString("".to_string())
    }
}

fn extended_client_test() {
    let cache_client = CacheClient::<CacheString, CacheString>::create_connect([127, 0, 0, 1], 8081).unwrap();
    let _s = CacheClient::<CacheString, CacheString>::cache_client_handler(&cache_client);
        // .join().unwrap();
    for i in 1..10 {
        let co = KeyValObj {
            key: CacheString(String::from(format!("key{}", i))),
            val: CacheString(String::from(format!("val{}", i))),
        };
        println!("test push {:?}", i);
        cache_client.push(co).unwrap();
    }
    
    let mut pull_k: CacheString;
    let mut pull_v: String;
    for i in 1..10 {
        // pull_k = CacheString(String::from("key2"));
        pull_k = CacheString(String::from(format!("key{}", i)));
        pull_v = String::from(format!("val{}", i));
        let get_res = cache_client.pull(&pull_k).unwrap();
        println!("received: {:}, {:}", get_res.key.0, get_res.val.0);
        assert_eq!(get_res.val.0, pull_v);
    }
    // if let Err(e) = s.join() {
    //     panic!("{:?}", e);
    // }
}

#[test]
fn extended_server_test() {
    let cache = CacheDb::<CacheString, CacheString>::new([127, 0, 0, 1], 8081);

    cache.push(KeyValObj {
        key: CacheString(String::from("brian")),
        val: CacheString(String::from("test")),
    });

    let _cache_db_server = CacheDb::<CacheString, CacheString>::cache_db_server(&cache);
        // .join().unwrap();
        
    thread::sleep(time::Duration::from_secs(1));
    extended_client_test();
    // cache_db_server.join().unwrap();
}
