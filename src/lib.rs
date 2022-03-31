use std::io::prelude::*;
use std::thread;
use std::net::{TcpStream, TcpListener, SocketAddr};
use std::marker::PhantomData;

pub enum CacheDbError {
    KeyNotFound,
}

pub enum ProtOpCode {
    PullOp = 0,
    PushOp = 1,
    PullReplyOp = 2
}

pub struct KeyValObj<KeyT, ValT> {
    key: KeyT,
    val: ValT,
}

pub struct CacheDb<KeyT, ValT> {
    ipv4_addr: [u8; 4],
    port: u16,

    key_val_store: Vec<Box<KeyValObj<KeyT, ValT>>>
}

pub struct CacheClient<KeyT, ValT> {
    ipv4_addr: [u8; 4],
    port: u16,

    tcp_conn: TcpStream,

    pd_k: PhantomData<KeyT>,
    pd_v: PhantomData<ValT>
}

pub trait GenericKeyVal {
    fn get_size(&self) -> usize;
    fn get_bytes(&self) -> Vec<u8>;
}

impl<KeyT, ValT> CacheClient<KeyT, ValT> where KeyT: GenericKeyVal, ValT: GenericKeyVal {
    pub fn create_connect(ipv4_addr: [u8;4], port: u16) ->  Result<CacheClient<KeyT, ValT>, std::io::Error> {
        let addr = SocketAddr::from((ipv4_addr, port));
        let tcp_stream = TcpStream::connect(addr);
        if let Err(e) = tcp_stream {
            return Err(e);
        } else if let Ok(stream) = tcp_stream {
            let cache_client = CacheClient {
                ipv4_addr: ipv4_addr, 
                port: port,
                tcp_conn: stream,
                pd_k: PhantomData,
                pd_v: PhantomData
            };
            return Ok(cache_client);
        }
        panic!("create_connect returned neither err nor succ");
    }

    pub fn push(&mut self, obj: KeyValObj<KeyT, ValT>) {
        let send_buff = CacheClient::assemble_send_buff(ProtOpCode::PushOp, obj);
        println!("test: {:?}", send_buff);
    }

    fn prot_op_code_to_u8(op_code: ProtOpCode) -> u8 {
        match op_code {
            ProtOpCode::PushOp => 2,
            ProtOpCode::PullOp => 1,
            ProtOpCode::PullReplyOp => 3,
        }
    }
    fn assemble_send_buff(op_code: ProtOpCode, obj: KeyValObj<KeyT, ValT>) -> Vec<u8> {
        let mut buff = Vec::<u8>::new();
        buff.push(CacheClient::<KeyT, ValT>::prot_op_code_to_u8(op_code));

        let key_size = obj.key.get_size().to_be_bytes();
        buff.extend_from_slice(&key_size);
        let mut key_bytes = obj.key.get_bytes();
        buff.append(&mut key_bytes);

        let val_size = obj.val.get_size().to_be_bytes();
        buff.extend_from_slice(&val_size);
        let mut val_bytes = obj.val.get_bytes();
        buff.append(&mut val_bytes);

        buff
    }
}


impl<KeyT: std::cmp::PartialEq, ValT> CacheDb<KeyT, ValT> {

    pub fn new(ipv4_addr: [u8;4], port: u16) -> CacheDb<KeyT, ValT> {
        let cache = CacheDb {
            ipv4_addr: ipv4_addr, 
            port: port,
            key_val_store: Vec::new()
        };
        cache
    }

    pub fn push(&mut self, obj: KeyValObj<KeyT, ValT>) {
        self.key_val_store.push(Box::<KeyValObj<KeyT, ValT>>::new(obj));
    }

    pub fn get(&self, key: KeyT) -> Option<&KeyValObj<KeyT, ValT>> {
        for obj in self.key_val_store.iter() {
            if obj.key == key {
                return Some(&obj);
            }
        }
        None   
    }

    pub fn set(&mut self, key: KeyT, val: ValT) -> Result<(), CacheDbError> {
        for obj in self.key_val_store.iter_mut() {
            if obj.key == key {
                obj.val = val;
                return Ok(());
            }
        }
        Err(CacheDbError::KeyNotFound)
    }

    fn client_handler(mut socket: TcpStream){
        let mut buf = [0; 10];
        socket.read(&mut buf).unwrap();
        socket.write(&[1]).unwrap();

        println!("read: {:?}", buf);
    }

    pub fn cache_db_server(&self) {
        let addr = SocketAddr::from((self.ipv4_addr, self.port));
        let listener = TcpListener::bind(addr).unwrap();

        loop {
            match listener.accept() {
                Ok((socket, addr)) => {
                    println!("new client: {:?}", addr);

                    let _handler = thread::spawn(move || (CacheDb::<KeyT, ValT>::client_handler(socket)));
                }
                Err(e) => println!("couldn't get client: {:?}", e),
            }    
            #[cfg(test)] 
            break;
        }
    }   
}


#[cfg(test)]
mod tests {
    use std::{thread, time};

    use super::*;

    impl GenericKeyVal for String {
        fn get_size(self: &String) -> usize {
            self.chars().count()
        }

        // must clone string since into_bytes() is not implemented for 
        // the string reference (at least not the copy trait which is required)
        fn get_bytes(self: &String) -> Vec<u8> {
            let str_bytes = self.clone();
            str_bytes.into_bytes()
        }
    }

    fn basic_client_test() {
        let mut cache_client = CacheClient::<String, String>::create_connect([127, 0, 0, 1], 8080).unwrap();
        cache_client.push(KeyValObj{key: String::from("brian"), val: String::from("test")});
    }

    #[test]
    fn basic_server_test() { 
        let cache = CacheDb::<String, String>::new([127, 0, 0, 1], 8080);
        let _test_server_instance = thread::spawn(move || (cache.cache_db_server()));
        thread::sleep(time::Duration::from_secs(1));
        basic_client_test();
    }

    #[test]
    fn local_cache_db_test() { 
        let mut cache = CacheDb::<String, String>::new([127, 0, 0, 1], 8080);

        cache.push(KeyValObj{key: String::from("brian"), val: String::from("test")});
        cache.push(KeyValObj{key: String::from("paul"), val: String::from("test")});
        cache.push(KeyValObj{key: String::from("pete"), val: String::from("test")});
        cache.push(KeyValObj{key: String::from("robert"), val: String::from("test")});

        let get_res = cache.get(String::from("brian")).unwrap();
        println!("get k: {} v: {}", get_res.key, get_res.val);
        let get_res = cache.get(String::from("paul")).unwrap();
        println!("get k: {} v: {}", get_res.key, get_res.val);
        let get_res = cache.get(String::from("pete")).unwrap();
        println!("get k: {} v: {}", get_res.key, get_res.val);
        let get_res = cache.get(String::from("robert")).unwrap();
        println!("get k: {} v: {}", get_res.key, get_res.val);

        if let None = cache.get(String::from("ian")) {
            println!("Ian not found!");
        } else {
            panic!("local get() test failed");
        }

        if let Err(_e) = cache.set(String::from("robert"), String::from("mod_test")) {
            panic!("set test failed");
        }
        let get_res = cache.get(String::from("robert")).unwrap();
        println!("mod get k: {} v: {}", get_res.key, get_res.val);

    }
}
