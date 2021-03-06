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

fn client_test_multiple_keys() {
    let cache_client = CacheClient::<CacheString, CacheString>::create_connect([127, 0, 0, 1], 8081).unwrap();
    let _s = CacheClient::<CacheString, CacheString>::cache_client_handler(&cache_client);
        // .join().unwrap();
    for i in 1..10 {
        let co = KeyValObj {
            key: CacheString(String::from(format!("key{}", i))),
            val: CacheString(String::from(format!("val{}", i))),
        };
        // println!("test push {:?}", i);
        cache_client.push(co).unwrap();
    }
    
    let mut pull_k: CacheString;
    let mut pull_v: String;
    let mut get_res = KeyValObj{key: CacheString(String::new()), val: CacheString(String::new())};
    for i in 1..10 {
        // pull_k = CacheString(String::from("key2"));
        pull_k = CacheString(String::from(format!("key{}", i)));
        pull_v = String::from(format!("val{}", i));
        cache_client.pull(&pull_k, &mut get_res).unwrap();
        // println!("received: {:}, {:}", get_res.key.0, get_res.val.0);
        assert_eq!(get_res.val.0, pull_v);
    }
    // if let Err(e) = s.join() {
    //     panic!("{:?}", e);
    // }
}

fn client_test_single_key() {
    let cache_client = CacheClient::<CacheString, CacheString>::create_connect([127, 0, 0, 1], 8081).unwrap();
    let _s = CacheClient::<CacheString, CacheString>::cache_client_handler(&cache_client);
        // .join().unwrap();
    
    let co = KeyValObj {
        key: CacheString("key".to_string()),
        val: CacheString("val".to_string()),
    };

    cache_client.push(co).unwrap();
    
    let pull_k = CacheString(String::from("key2"));
    
    let cache_client_clone = cache_client.clone();
    let pull_k_clone = pull_k.clone();
    thread::spawn(move || {
        let mut i = 0;
        let mut get_res = KeyValObj{key: CacheString(String::new()), val: CacheString(String::new())};  
        for _ in 1..500 {
            cache_client_clone.pull(&pull_k_clone, &mut get_res).unwrap();
            // println!("received: {:}, {:}", get_res.key.0, get_res.val.0);
            i += 1;        
        }
        assert_eq!(499, i);
    });
    let mut get_res = KeyValObj{key: CacheString(String::new()), val: CacheString(String::new())};
    let mut i = 0;
    for _ in 1..500 {
        cache_client.pull(&pull_k, &mut get_res).unwrap();
        i += 1;
    }
    assert_eq!(499, i);

    // if let Err(e) = s.join() {
    //     panic!("{:?}", e);
    // }
}

fn client_test_single_key_async() {
    let cache_client = CacheClient::<CacheString, CacheString>::create_connect([127, 0, 0, 1], 8081).unwrap();
    let _s = CacheClient::<CacheString, CacheString>::cache_client_handler(&cache_client);
        // .join().unwrap();
    
    let co = KeyValObj {
        key: CacheString("key".to_string()),
        val: CacheString("val".to_string()),
    };

    cache_client.push(co).unwrap();
    
    let pull_k = CacheString(String::from("key2"));

    let mut i = 0;
    for _ in 1..500 {
        let _get_res = CacheClient::pull_async(&cache_client, &pull_k);
        i += 1;
        // join thread and wait
    }
    assert_eq!(499, i);

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
    client_test_multiple_keys();
    client_test_single_key();
    client_test_single_key_async();
    // cache_db_server.join().unwrap();
}
