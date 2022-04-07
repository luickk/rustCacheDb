use std::io::prelude::*;
use std::io;
use std::fmt::Debug;
use std::thread;
use std::net::{TcpStream, TcpListener, SocketAddr};
use std::marker::PhantomData;

const TCP_READ_BUFF_SIZE: usize = 1024;

#[derive(Debug)] 
pub enum CacheDbError {
    KeyNotFound,
    ParsingErr,
    DecodingErr,
    ProtocolSizeBufferOverflow,
    NetworkError,
}

#[derive(PartialEq, Clone, Copy, Debug)]
pub enum ProtOpCode {
    PullOp = 0,
    PushOp = 1,
    PullReplyOp = 2
}

pub struct KeyValObj<KeyT, ValT> {
    pub key: KeyT,
    pub val: ValT,
}

pub struct CacheDb<KeyT, ValT> {
    ipv4_addr: [u8; 4],
    port: u16,

    key_val_store: Vec<Box<KeyValObj<KeyT, ValT>>>
}

pub struct CacheProtocol<KeyT, ValT> {
    // points to the segment currently read
    parsed_protocoll_segment: u8,
    parsed_bytes_total: usize,
    to_parse_bytes_total: usize,
    key_size: u16,
    val_size: u16,

    pd_k: PhantomData<KeyT>,
    pd_v: PhantomData<ValT>
}


pub struct CacheClient<KeyT, ValT> {
    tcp_conn: TcpStream,

    pd_k: PhantomData<KeyT>,
    pd_v: PhantomData<ValT>
}

pub trait GenericKeyVal<Val> {
    fn get_size(&self) -> Result<u16, CacheDbError>;
    fn get_bytes(&self) -> Vec<u8>;
    fn from_bytes(data: &[u8]) -> Result<Val, CacheDbError>;
}


impl<KeyT, ValT> CacheProtocol<KeyT, ValT> where KeyT: GenericKeyVal<KeyT>, ValT: GenericKeyVal<ValT> {
    fn prot_op_code_to_u8(op_code: &ProtOpCode) -> u8 {
        match op_code {
            ProtOpCode::PushOp => 2,
            ProtOpCode::PullOp => 1,
            ProtOpCode::PullReplyOp => 3,
        }
    }
    fn u8_to_prot_op_code(op_code: u8) -> Option<ProtOpCode> {
        match op_code {
            2 => Some(ProtOpCode::PushOp),
            1 => Some(ProtOpCode::PullOp),
            3 => Some(ProtOpCode::PullReplyOp),
            _ => None,
        }
    }

    pub fn assemble_buff(op_code: ProtOpCode, obj: &KeyValObj<KeyT, ValT>) -> Result<Vec<u8>, CacheDbError> {
        let mut buff = Vec::<u8>::new();
        buff.push(CacheProtocol::<KeyT, ValT>::prot_op_code_to_u8(&op_code));

        let key_size = obj.key.get_size()?.to_be_bytes();
        buff.extend_from_slice(&key_size);
        let mut key_bytes = obj.key.get_bytes();
        buff.append(&mut key_bytes);

        let mut val_size: [u8; 2] = [0,0];
        if op_code != ProtOpCode::PullOp {
            val_size = obj.val.get_size()?.to_be_bytes();
        }
        buff.extend_from_slice(&val_size);
        if op_code != ProtOpCode::PullOp {
            let mut val_bytes = obj.val.get_bytes();
            buff.append(&mut val_bytes);
        }

        Ok(buff)
    }


    pub fn new() -> CacheProtocol<KeyT, ValT> {
        CacheProtocol{
            parsed_protocoll_segment: 0,
            parsed_bytes_total: 0,
            to_parse_bytes_total: 0,
            key_size: 0,
            val_size: 0,
            pd_k: PhantomData,
            pd_v: PhantomData
        }
    }

    // things to notice: tcp data can come in at different sizes(only order is guaranteed)
    // so this parsing method tries to account for that by keeping states
    pub fn parse_buff(&mut self, buff: &[u8; TCP_READ_BUFF_SIZE], tcp_read_size: usize, op_code: &mut ProtOpCode, obj: &mut KeyValObj<KeyT, ValT>) -> Result<(bool, usize), CacheDbError>{
        let mut key_valsize_raw: [u8; 2] = [0; 2];
        let mut op_code_raw: u8;

        loop {
            match self.parsed_protocoll_segment {
                // parsing protocol op_code
                0 if { tcp_read_size >= 1 } => {
                    op_code_raw = buff[0];
                    if let Some(op_code_en) = &CacheProtocol::<KeyT, ValT>::u8_to_prot_op_code(op_code_raw) {
                        op_code.clone_from(op_code_en);
                    } else {
                        return Err(CacheDbError::ParsingErr);
                    }
                    self.parsed_protocoll_segment += 1;

                    self.parsed_bytes_total += 1;
                    self.to_parse_bytes_total += 3;
                },
                // parsing protocol key size
                1 if { tcp_read_size >= self.to_parse_bytes_total } => {
                    key_valsize_raw.copy_from_slice(&buff[self.parsed_bytes_total..self.to_parse_bytes_total]);
                    self.key_size = u16::from_be_bytes(key_valsize_raw);
                    self.parsed_protocoll_segment += 1;

                    self.parsed_bytes_total = self.to_parse_bytes_total;
                    self.to_parse_bytes_total += usize::from(self.key_size);
                },
                // parsing protocol key
                2 if { tcp_read_size >= self.to_parse_bytes_total } => {
                    let mut data = Vec::<u8>::new();
                    data.extend_from_slice(&buff[self.parsed_bytes_total..self.to_parse_bytes_total]);
                    obj.key = KeyT::from_bytes(&data)?;
                    self.parsed_protocoll_segment += 1;

                    self.parsed_bytes_total = self.to_parse_bytes_total;
                    self.to_parse_bytes_total += 2 ;
                },
                // parsing protocol val size
                3 if { tcp_read_size >= self.to_parse_bytes_total } => {
                    key_valsize_raw.copy_from_slice(&buff[self.parsed_bytes_total..self.to_parse_bytes_total]);
                    self.val_size = u16::from_be_bytes(key_valsize_raw);
                    self.parsed_protocoll_segment += 1;

                    self.parsed_bytes_total = self.to_parse_bytes_total;
                    self.to_parse_bytes_total += usize::from(self.val_size);
                },
                // parsing protocol val
                4 if { tcp_read_size >= self.to_parse_bytes_total } => {
                    let mut data = Vec::<u8>::new();
                    data.extend_from_slice(&buff[self.parsed_bytes_total..self.to_parse_bytes_total]);
                    obj.val = ValT::from_bytes(&data)?;

                    // using self.to_parse_bytes_total because next matching arm (this one) has been parsed
                    let left_over = tcp_read_size-self.to_parse_bytes_total;

                    self.parsed_protocoll_segment = 0;
                    self.parsed_bytes_total = 0;
                    self.to_parse_bytes_total = 0;
                    return Ok((true, left_over));
                },
                _ => {
                    // using self.parsed_bytes_total because thats last matching expression and self.to_parse_bytes_total isn't parsed anymore
                    let left_over = tcp_read_size-self.parsed_bytes_total;
                    return Ok((false, left_over));
                }
            }
        }
    }
}

impl<KeyT, ValT> CacheClient<KeyT, ValT> where KeyT: GenericKeyVal<KeyT>, ValT: GenericKeyVal<ValT> {
    pub fn create_connect(ipv4_addr: [u8;4], port: u16) ->  Result<CacheClient<KeyT, ValT>, std::io::Error> {
        let addr = SocketAddr::from((ipv4_addr, port));
        let tcp_stream = TcpStream::connect(addr)?;
        let cache_client = CacheClient {
            tcp_conn: tcp_stream,
            pd_k: PhantomData,
            pd_v: PhantomData
        };
        return Ok(cache_client);
    }

    pub fn push(&mut self, obj: KeyValObj<KeyT, ValT>) -> Result<(), CacheDbError> {
        let send_buff = CacheProtocol::assemble_buff(ProtOpCode::PushOp, &obj)?;
        if let Err(_) = self.tcp_conn.write(&send_buff) {
            return Err(CacheDbError::NetworkError);
        }
        Ok(())
    }
}


impl<KeyT, ValT> CacheDb<KeyT, ValT> where KeyT: std::cmp::PartialEq + GenericKeyVal<KeyT> + Default + Debug, ValT: GenericKeyVal<ValT> + Default + Debug {

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

    fn client_handler(mut socket: TcpStream) {
        let mut buff = [0; TCP_READ_BUFF_SIZE];

        let mut parser = CacheProtocol::<KeyT, ValT>::new();
        let mut parsed_op_code: ProtOpCode = ProtOpCode::PullOp;
        let mut parsed_obj: KeyValObj<KeyT, ValT> = KeyValObj { key: KeyT::default(), val: ValT::default() };
        let mut tcp_read_size: usize;
        let mut n = 0;
        loop {
            tcp_read_size = socket.read(&mut buff).unwrap();
            if tcp_read_size == 0 {
                continue;
            }
            loop {
                let (parsed, left_over) = parser.parse_buff(&buff, tcp_read_size, &mut parsed_op_code, &mut parsed_obj).unwrap();
                if left_over != 0 && !parsed {
                    break;
                } else if parsed {
                    n += 1;
                    println!("{:?} - OpCode: {:?} Key: {:?}, Val: {:?}", n, parsed_op_code, parsed_obj.key, parsed_obj.val);
                    continue;
                }
            }
            #[cfg(test)]
            break;
        }
    }

    pub fn cache_db_server(&self) -> io::Result<()>{
        let addr = SocketAddr::from((self.ipv4_addr, self.port));
        let listener = TcpListener::bind(addr)?;

        loop {
            let (socket, _addr) = listener.accept()?;
            let _handler = thread::spawn(move || (CacheDb::<KeyT, ValT>::client_handler(socket)));
            
            #[cfg(test)] 
            return Ok(());
        }
    }   
}


#[cfg(test)]
mod tests {
    use std::{thread, time};

    use super::*;

    impl GenericKeyVal<String> for String {
        fn get_size(self: &String) -> Result<u16, CacheDbError> {
            match self.chars().count().try_into() {
                Ok(size) => {
                    return Ok(size);
                },
                Err(_) => {
                    return Err(CacheDbError::ProtocolSizeBufferOverflow);
                }
            }
        }

        // must clone string since into_bytes() is not implemented for 
        // the string reference (at least not the copy trait which is required)
        fn get_bytes(self: &String) -> Vec<u8> {
            let str_bytes = self.clone();
            str_bytes.into_bytes()
        }

        fn from_bytes(data: &[u8]) -> Result<String, CacheDbError> {
            if let Ok(data_str) = std::str::from_utf8(data) {
                return Ok(data_str.to_string());
            }
            Err(CacheDbError::DecodingErr)
        }
    }

    fn basic_client_test() {
        let mut cache_client = CacheClient::<String, String>::create_connect([127, 0, 0, 1], 8080).unwrap();
        cache_client.push(KeyValObj{key: String::from("brian"), val: String::from("test")}).unwrap();
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
