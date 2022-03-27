use std::io::prelude::*;
use std::thread;
use std::net::{TcpStream, TcpListener, SocketAddr};

pub struct KeyValObj<KeyT, ValT> {
    key: KeyT,
    val: ValT,
}

pub struct CacheDb<KeyT, ValT> {
    ipv4_addr: [u8; 4],
    port: u16,
    cache_size: u16,

    key_val_store: Vec<Box<KeyValObj<KeyT, ValT>>>
}

impl<KeyT, ValT> CacheDb<KeyT, ValT> {

    pub fn new(ipv4_addr: [u8;4], port: u16, cache_size: u16) -> CacheDb<KeyT, ValT> {
        let cache = CacheDb {
            ipv4_addr: ipv4_addr, 
            port: port,
            cache_size: cache_size,
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

    fn client_handler(mut socket: TcpStream){
        let mut buf = [0; 10];
        socket.read(&mut buf).unwrap();
        socket.write(&[1]).unwrap();

        println!("read: {:?}", buf);
    }

    pub fn server_client_handler(&self) {
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

    fn basic_client_test() {
        let mut stream = TcpStream::connect("127.0.0.1:8080").unwrap();
        stream.write(&[1]).unwrap();
    }

    #[test]
    fn server_test() { 
        let cache = CacheDb::<String, String>::new([127, 0, 0, 1], 8080, 200);
        let _test_server_instance = thread::spawn(move || (cache.server_client_handler()));
        thread::sleep(time::Duration::from_secs(1));
        basic_client_test();
    }
}
