use std::fmt::Debug;
use std::io;
use std::io::prelude::*;
use std::marker::PhantomData;
use std::thread;
use std::thread::JoinHandle;
use std::cmp::PartialEq;
use std::time::Duration;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, RwLock, Mutex, Condvar};
use std::marker::{Send, Sync};

const TCP_READ_BUFF_SIZE: usize = 1024;
const CACHE_CLIENT_REQ_SIG_WAIT: Duration = Duration::from_secs(10);

// todo => remove potentially unnecessary iterations over the key_val_stores (benchmarks)

#[derive(Debug, PartialEq)]
pub enum CacheDbError {
    KeyNotFound,
    ParsingErr,
    DecodingErr,
    ProtocolSizeBufferOverflow,
    NetworkError,
    NetworkTimeOutError
}

#[derive(PartialEq, Clone, Copy, Debug)]
pub enum ProtOpCode {
    PullOp = 1,
    PushOp = 2,
    PullReplyOp = 3,
    PullReplyNotFoundOp = 4,
    TerminateConn = 5,
}

#[derive(Clone, Copy)]
pub struct KeyValObj<KeyT, ValT> {
    pub key: KeyT,
    pub val: ValT,
}

// 0 contains the key_val pair, 1 indicates wether the key could be found 
pub struct KeyValObjSyncLocked<KeyT, ValT>(KeyValObj<KeyT, ValT>, bool);
pub struct KeyValObjSync<KeyT, ValT> {
    // only true if data is currently requested (pulled)
    pub pulling: Mutex<bool>,
    pub pulling_sig: Condvar,

    pub key_val: RwLock<KeyValObjSyncLocked<KeyT, ValT>>
}

pub struct CacheDb<KeyT, ValT> {
    ipv4_addr: [u8; 4],
    port: u16,

    key_val_store: RwLock<Vec<Box<KeyValObj<KeyT, ValT>>>>
}

pub struct CacheProtocol<KeyT, ValT> {
    // points to the segment currently read
    parsed_protocoll_segment: u8,
    parsed_bytes_total: usize,
    to_parse_bytes_total: usize,
    key_size: u16,
    val_size: u16,

    // because of unconstrained type conflict
    pd_k: PhantomData<KeyT>,
    pd_v: PhantomData<ValT>,
}

pub struct CacheClient<KeyT, ValT> {
    key_val_sync_store: RwLock<Vec<KeyValObjSync<KeyT, ValT>>>,
    tcp_conn: RwLock<TcpStream>,

    // because of unconstrained type conflict
    pd_k: PhantomData<KeyT>,
    pd_v: PhantomData<ValT>,
}

pub trait GenericKeyVal<Val> {
    fn get_size(&self) -> Result<u16, CacheDbError>;
    fn get_bytes(&self) -> Vec<u8>;
    fn from_bytes(data: &[u8]) -> Result<Val, CacheDbError>;
}

impl<KeyT, ValT> CacheProtocol<KeyT, ValT> where KeyT: GenericKeyVal<KeyT>, ValT: GenericKeyVal<ValT> + Debug {
    fn prot_op_code_to_u8(op_code: &ProtOpCode) -> u8 {
        match op_code {
            ProtOpCode::PullOp => u8::from_le(1),
            ProtOpCode::PushOp => u8::from_le(2),
            ProtOpCode::PullReplyOp => u8::from_le(3),
            ProtOpCode::PullReplyNotFoundOp => u8::from_le(4),
            ProtOpCode::TerminateConn => u8::from_le(5),
        }
    }
    fn u8_to_prot_op_code(op_code: u8) -> Option<ProtOpCode> {
        match u8::from_be(op_code) {
            1 => Some(ProtOpCode::PullOp),
            2 => Some(ProtOpCode::PushOp),
            3 => Some(ProtOpCode::PullReplyOp),
            4 => Some(ProtOpCode::PullReplyNotFoundOp),
            5 => Some(ProtOpCode::TerminateConn),
            _ => None,
        }
    }

    pub fn assemble_buff(op_code: ProtOpCode, obj: &KeyValObj<KeyT, ValT>) -> Result<Vec<u8>, CacheDbError> {
        let mut buff = Vec::<u8>::new();
        buff.push(CacheProtocol::<KeyT, ValT>::prot_op_code_to_u8(&op_code));
        let key_size = obj.key.get_size()?;
        buff.extend_from_slice(&u16::from_le(key_size).to_be_bytes());
        let mut key_bytes = obj.key.get_bytes();
        buff.append(&mut key_bytes);

        if op_code != ProtOpCode::PullOp {
            buff.extend_from_slice(&u16::from_le(obj.val.get_size()?).to_be_bytes());
        } else {
            buff.extend_from_slice(&[0_u8, 0_u8]);
        }
        if op_code != ProtOpCode::PullOp {
            let mut val_bytes = obj.val.get_bytes();
            buff.append(&mut val_bytes);
        }

        Ok(buff)
    }

    pub fn new() -> CacheProtocol<KeyT, ValT> {
        CacheProtocol {
            parsed_protocoll_segment: 0,
            parsed_bytes_total: 0,
            to_parse_bytes_total: 0,
            key_size: 0,
            val_size: 0,
            pd_k: PhantomData,
            pd_v: PhantomData,
        }
    }

    // things to notice: tcp data can come in at different sizes(only order is guaranteed - FIFO)
    // so this parsing method tries to account for that by keeping states
    pub fn parse_buff(&mut self, buff: &mut [u8; TCP_READ_BUFF_SIZE], tcp_read_size: usize, op_code: &mut ProtOpCode, obj: &mut KeyValObj<KeyT, ValT>) -> Result<(bool, usize), CacheDbError> {
        let mut key_valsize_raw: [u8; 2] = [0; 2];
        let mut op_code_raw: u8;

        loop {
            match self.parsed_protocoll_segment {
                // parsing protocol op_code
                0 if { tcp_read_size >= self.parsed_bytes_total+1 } => {
                    self.to_parse_bytes_total +=1;
                    op_code_raw = buff[self.parsed_bytes_total];

                    if let Some(op_code_en) = &CacheProtocol::<KeyT, ValT>::u8_to_prot_op_code(op_code_raw) {
                        op_code.clone_from(op_code_en);
                    } else {
                        return Err(CacheDbError::ParsingErr);
                    }
                    self.parsed_protocoll_segment += 1;

                    self.parsed_bytes_total = self.to_parse_bytes_total;
                    self.to_parse_bytes_total += 2;
                }
                // parsing protocol key size
                1 if { tcp_read_size >= self.to_parse_bytes_total } => {
                    key_valsize_raw.copy_from_slice(&buff[self.parsed_bytes_total..self.to_parse_bytes_total]);
                    self.key_size = u16::from_be_bytes(key_valsize_raw);
                    self.parsed_protocoll_segment += 1;

                    self.parsed_bytes_total = self.to_parse_bytes_total;
                    self.to_parse_bytes_total += usize::from(self.key_size);
                }
                // parsing protocol key
                2 if { tcp_read_size >= self.to_parse_bytes_total } => {
                    if self.key_size != 0 {
                        obj.key = KeyT::from_bytes(&buff[self.parsed_bytes_total..self.to_parse_bytes_total])?;
                    }
                    self.parsed_protocoll_segment += 1;

                    self.parsed_bytes_total = self.to_parse_bytes_total;
                    self.to_parse_bytes_total += 2;
                }
                // parsing protocol val size
                3 if { tcp_read_size >= self.to_parse_bytes_total } => {
                    key_valsize_raw.copy_from_slice(&buff[self.parsed_bytes_total..self.to_parse_bytes_total]);
                    self.val_size = u16::from_be_bytes(key_valsize_raw);
                    self.parsed_protocoll_segment += 1;

                    self.parsed_bytes_total = self.to_parse_bytes_total;
                    self.to_parse_bytes_total += usize::from(self.val_size);
                }
                // parsing protocol val
                4 if { tcp_read_size >= self.to_parse_bytes_total } => {
                    if self.val_size != 0 {
                        obj.val = ValT::from_bytes(&buff[self.parsed_bytes_total..self.to_parse_bytes_total])?;
                    }
                    self.parsed_protocoll_segment = 0;
                    self.parsed_bytes_total = self.to_parse_bytes_total;

                    // self.parsed_bytes_total = 0;
                    // self.to_parse_bytes_total = 0;
                    return Ok((true, 0));
                }
                _ => {
                    // using self.parsed_bytes_total because thats last matching expression and self.to_parse_bytes_total isn't parsed anymore
                    let left_over_size = tcp_read_size - self.parsed_bytes_total;

                    if left_over_size > 0 {
                        // cloning to new vec because "inside buff copy" or move would require a mut and non mut ref
                        let mut left_over_buff: Vec<u8> = vec![];
                        left_over_buff.extend_from_slice(&buff[(tcp_read_size-left_over_size)..]);

                        // copying left over to the beginning of the array. (will not be overwritten at nect tcp read!)
                        buff[..left_over_size].copy_from_slice(&left_over_buff);
                    }
                    return Ok((false, left_over_size));
                }
            }
        }
    }
}

impl<KeyT: 'static, ValT: 'static> CacheClient<KeyT, ValT> where KeyT: GenericKeyVal<KeyT> + Clone + PartialEq + Default + Debug + Send + Sync, ValT: GenericKeyVal<ValT> + Clone + Debug + Default + Send + Sync {

    pub fn create_connect(ipv4_addr: [u8; 4], port: u16) -> Result<Arc<CacheClient<KeyT, ValT>>, std::io::Error> {
        let addr = SocketAddr::from((ipv4_addr, port));
        let tcp_stream = TcpStream::connect(addr)?;
        Ok(Arc::new(CacheClient {
            tcp_conn: RwLock::new(tcp_stream),
            key_val_sync_store: RwLock::new(Vec::new()),

            pd_k: PhantomData,
            pd_v: PhantomData
        }))
    }

    pub fn push(&self, obj: KeyValObj<KeyT, ValT>) -> Result<(), CacheDbError> {
        let send_buff = CacheProtocol::assemble_buff(ProtOpCode::PushOp, &obj)?;
        if let Err(_) = self.tcp_conn.write().unwrap().write(&send_buff) {
            return Err(CacheDbError::NetworkError);
        }
        Ok(())
    }

    pub fn pull(&self, key: &KeyT, res: &mut KeyValObj<KeyT, ValT>) -> Result<(), CacheDbError> {
        loop {
            for obj in self.key_val_sync_store.read().unwrap().iter() {
                if obj.key_val.read().unwrap().0.key == *key {
                    let mut _pull_sig_lock = obj.pulling.lock().unwrap();

                    // if it is not yet requesting val; we do so
                    // if is was already requested(obj.pulling = true) by somebody else we don't need to repeat
                    if !*_pull_sig_lock {
                        let send_buff = CacheProtocol::assemble_buff(ProtOpCode::PullOp, &KeyValObj{key: (*key).clone(), val: ValT::default()})?;
                        if let Err(_) = self.tcp_conn.write().unwrap().write(&send_buff) {
                            return Err(CacheDbError::NetworkError);
                        }

                        // is set back to negative by the cache_client_handler on request reply
                        *_pull_sig_lock = true;
                        obj.pulling_sig.notify_one();
                    }

                    // waiting for pulling to turn to false
                    // if block above ensures that this loop is reached 1. only if there has been a request made by this method (if !obj.pulling) or
                    // 2. if it was true (obj.pulling), it has been true already which indicates that there has been a request made by somebody else
                    while *_pull_sig_lock {
                        let _pull_sig_lock = obj.pulling_sig.wait_timeout(_pull_sig_lock, CACHE_CLIENT_REQ_SIG_WAIT).unwrap();
                        if _pull_sig_lock.1.timed_out() {
                            return Err(CacheDbError::NetworkTimeOutError);
                        }
                        if obj.key_val.read().unwrap().1 {
                            return Err(CacheDbError::KeyNotFound);
                        }

                        // obj.pulling has been set to false by the cache_client_handler and can now be read from the key_val_sync_store
                        *res = KeyValObj{key: obj.key_val.read().unwrap().0.key.clone(), val: obj.key_val.read().unwrap().0.val.clone()};
                        return Ok(());
                    }
                }
            }
            // print!("created new one");
            self.key_val_sync_store.write().unwrap().push(KeyValObjSync{pulling: Mutex::new(false), pulling_sig: Condvar::new(), key_val: RwLock::new(KeyValObjSyncLocked(KeyValObj{key: (*key).clone(), val: ValT::default()}, false))});            
        }
    }

    pub fn pull_async(cache_client: &Arc<CacheClient<KeyT, ValT>>, key: &KeyT) -> JoinHandle<Result<KeyValObj<KeyT, ValT>, CacheDbError>> {
        let cache_client = cache_client.clone();
        let key = (*key).clone();
        return thread::spawn(move || {
            loop {
                for obj in cache_client.key_val_sync_store.read().unwrap().iter() {
                    if obj.key_val.read().unwrap().0.key == key {
                        let mut _pull_sig_lock = obj.pulling.lock().unwrap();

                        // if it is not yet requesting val; we do so
                        // if is was already requested(obj.pulling = true) by somebody else we don't need to repeat
                        if !*_pull_sig_lock {
                            let send_buff = CacheProtocol::assemble_buff(ProtOpCode::PullOp, &KeyValObj{key: key.clone(), val: ValT::default()})?;
                            if let Err(_) = cache_client.tcp_conn.write().unwrap().write(&send_buff) {
                                return Err(CacheDbError::NetworkError);
                            }

                            // is set back to negative by the cache_client_handler on request reply
                            *_pull_sig_lock = true;
                            obj.pulling_sig.notify_one();
                        }

                        // waiting for pulling to turn to false
                        // if block above ensures that this loop is reached 1. only if there has been a request made by this method (if !obj.pulling) or
                        // 2. if it was true (obj.pulling), it has been true already which indicates that there has been a request made by somebody else
                        while *_pull_sig_lock {
                            let _pull_sig_lock = obj.pulling_sig.wait_timeout(_pull_sig_lock, CACHE_CLIENT_REQ_SIG_WAIT).unwrap();
                            if _pull_sig_lock.1.timed_out() {
                                return Err(CacheDbError::NetworkTimeOutError);
                            }
                            // obj.pulling has been set to false by the cache_client_handler and can now be read from the key_val_sync_store
                            return Ok(KeyValObj{key: obj.key_val.read().unwrap().0.key.clone(), val: obj.key_val.read().unwrap().0.val.clone()});
                        }
                        return Err(CacheDbError::NetworkError);
                    }
                }
                cache_client.key_val_sync_store.write().unwrap().push(KeyValObjSync{pulling: Mutex::new(false), pulling_sig: Condvar::new(), key_val: RwLock::new(KeyValObjSyncLocked(KeyValObj{key: key.clone(), val: ValT::default()}, false))});
            }
        });
    }


    pub fn terminate_conn(&mut self) -> io::Result<usize> {
        // op_code 4 -> terminate_conn
        // key/val size 0/ 0
        let term_seq: [u8; 5] = [4, 0, 0 ,0 ,0];
        self.tcp_conn.write().unwrap().write(&term_seq)
    }

    pub fn cache_client_handler(cache_client: &Arc<CacheClient<KeyT, ValT>>) -> JoinHandle<Result<(), CacheDbError>> {
        let ccache_clone = Arc::clone(cache_client);
        thread::spawn(move || {
            let mut buff = [0; TCP_READ_BUFF_SIZE];

            let mut parser = CacheProtocol::<KeyT, ValT>::new();
            let mut parsed_op_code: ProtOpCode = ProtOpCode::PullOp;
            let mut parsed_obj: KeyValObj<KeyT, ValT> = KeyValObj {
                key: KeyT::default(),
                val: ValT::default(),
            };
            let mut tcp_read_size: usize;
            let mut buff_left_over_size: usize = 0;
            let mut cloned_socket = ccache_clone.tcp_conn.write().unwrap().try_clone().unwrap();
            'tcp_read: loop {
                if buff_left_over_size == 0 {
                    match cloned_socket.read(&mut buff) {
                        Err(_) => return Err(CacheDbError::NetworkError),
                        Ok(size) => tcp_read_size = size
                    }
                } else {
                    match cloned_socket.read(&mut buff[buff_left_over_size..]) {
                        Err(_) => return Err(CacheDbError::NetworkError),
                        Ok(size) => tcp_read_size = size
                    }
                }
                
                if tcp_read_size == 0 {
                    continue;
                }

                tcp_read_size += buff_left_over_size;
                
                // reset buffer parsing pointer only if there is no left over buffer and 
                // if the parsing is completed
                if buff_left_over_size == 0 && parser.parsed_protocoll_segment == 0 {
                    parser.parsed_bytes_total = 0;
                    parser.to_parse_bytes_total = 0;
                } else {
                    // set to_parse_bytes_total to absolute size
                    parser.to_parse_bytes_total = parser.to_parse_bytes_total - parser.parsed_bytes_total;
                    parser.parsed_bytes_total = 0;
                }

                loop {
                    match parser.parse_buff(&mut buff, tcp_read_size, &mut parsed_op_code, &mut parsed_obj)? {
                        // check wether parse_buff is done(-> can't parse the buffer any further without next tcp buff read)
                        (parsed, left_over_size) if !parsed => {
                            buff_left_over_size = left_over_size;
                            break;
                        },
                        (parsed, _) if parsed => {
                            // successfully parsed parsed_obj is now updated to latest parsed obj (such as parsed_op_code)
                            match parsed_op_code {
                                ProtOpCode::TerminateConn => {
                                    break 'tcp_read;
                                },
                                ProtOpCode::PullReplyOp | ProtOpCode::PullReplyNotFoundOp => {
                                    for obj in ccache_clone.key_val_sync_store.read().unwrap().iter() {
                                        if obj.key_val.read().unwrap().0.key == parsed_obj.key {
                                            obj.key_val.write().unwrap().0.val = parsed_obj.val.clone();
                                            if parsed_op_code == ProtOpCode::PullReplyNotFoundOp {
                                                obj.key_val.write().unwrap().1 = true;
                                            }
                                            *obj.pulling.lock().unwrap() = false;
                                            obj.pulling_sig.notify_one();
                                        }
                                    }
                                },
                                _ => {
                                    break 'tcp_read;
                                }
                            }
                            continue;
                        },
                        (_, _) => {
                            break;
                        }
                    }
                }
            }
            return Ok(())
        })
    }
}

impl<KeyT: 'static, ValT: 'static> CacheDb<KeyT, ValT> where KeyT: PartialEq + GenericKeyVal<KeyT> + Default + Debug + Send + Sync + Clone, ValT: GenericKeyVal<ValT> + Default + Debug + Send + Sync, KeyValObj<KeyT, ValT>: Clone {
    pub fn new(ipv4_addr: [u8; 4], port: u16) -> Arc<CacheDb<KeyT, ValT>> {
        let cache = CacheDb {
            ipv4_addr: ipv4_addr,
            port: port,
            key_val_store: RwLock::new(Vec::new()),
        };
        Arc::new(cache)
    }

    pub fn push(&self, obj: KeyValObj<KeyT, ValT>) {
        self.key_val_store.write().unwrap().push(Box::<KeyValObj<KeyT, ValT>>::new(obj));

    }

    // returning reference since there is a lifetime from the CacheDb(self) struct
    pub fn get(&self, key: &KeyT) -> Option<KeyValObj<KeyT, ValT>> {
        for obj in self.key_val_store.read().unwrap().iter() {
            if &obj.key == key {
                return Some((**obj).clone());
            }
        }
        None
    }

    pub fn set(&self, key: KeyT, val: ValT) -> Result<(), CacheDbError> {
        for obj in self.key_val_store.write().unwrap().iter_mut() {
            if obj.key == key {
                obj.val = val;
                return Ok(());
            }
        }
        Err(CacheDbError::KeyNotFound)
    }

    fn client_handler(mut socket: TcpStream, cache: &Arc<CacheDb<KeyT, ValT>>) {
        let mut buff = [0; TCP_READ_BUFF_SIZE];

        let mut parser = CacheProtocol::<KeyT, ValT>::new();
        let mut parsed_op_code: ProtOpCode = ProtOpCode::PullOp;
        let mut parsed_obj: KeyValObj<KeyT, ValT> = KeyValObj {
            key: KeyT::default(),
            val: ValT::default(),
        };
        let mut tcp_read_size: usize;
        let mut buff_left_over_size: usize = 0;
        'tcp_read: loop {
            if buff_left_over_size == 0 {
                tcp_read_size = socket.read(&mut buff).unwrap();

            } else {
                tcp_read_size = socket.read(&mut buff[buff_left_over_size..]).unwrap();
            }
            
            if tcp_read_size == 0 {
                continue;
            }

            tcp_read_size += buff_left_over_size;
            
            // reset buffer parsing pointer only if there is no left over buffer and 
            // if the parsing is completed
            if buff_left_over_size == 0 && parser.parsed_protocoll_segment == 0 {
                parser.parsed_bytes_total = 0;
                parser.to_parse_bytes_total = 0;
            } else {
                // set to_parse_bytes_total to absolute size
                parser.to_parse_bytes_total = parser.to_parse_bytes_total - parser.parsed_bytes_total;
                parser.parsed_bytes_total = 0;
            }

            loop {
                match parser.parse_buff(&mut buff, tcp_read_size, &mut parsed_op_code, &mut parsed_obj).unwrap() {
                    // check wether parse_buff is done(-> can't parse the buffer any further without next tcp buff read)
                    (parsed, left_over_size) if !parsed => {
                        buff_left_over_size = left_over_size;
                        break;
                    },
                    (parsed, _) if parsed => {
                        // successfully parsed parsed_obj is now updated to latest parsed obj (such as parsed_op_code)
                        match parsed_op_code {
                            ProtOpCode::TerminateConn => {
                                break 'tcp_read;
                            },
                            ProtOpCode::PushOp => {
                                cache.push(parsed_obj.clone());
                            }
                            ProtOpCode::PullOp => {
                                match cache.get(&parsed_obj.key) {
                                    Some(obj) => {
                                        match CacheProtocol::assemble_buff(ProtOpCode::PullReplyOp, &obj) {
                                            Ok(send_buff) => {
                                                if let Err(_) = socket.write(&send_buff) {
                                                    break 'tcp_read;
                                                }       
                                            },
                                            Err(_) => {
                                                break 'tcp_read;
                                            }
                                        }    
                                    }
                                    None => {
                                        match CacheProtocol::assemble_buff(ProtOpCode::PullReplyNotFoundOp, &KeyValObj{key: parsed_obj.key.clone(), val: ValT::default()}) {
                                            Ok(send_buff) => {
                                                if let Err(_) = socket.write(&send_buff) {
                                                    break 'tcp_read;
                                                }       
                                            },
                                            Err(_) => {
                                                break 'tcp_read;
                                            }
                                        }
                                    }
                                }
                            },
                            _ => {
                                break 'tcp_read;
                            }
                        }
                        continue;
                    },
                    (_, _) => {
                        break;
                    }
                }
            }
        }
    }

    pub fn cache_db_server(cache: &Arc<CacheDb<KeyT, ValT>>) -> JoinHandle<io::Result<()>> {
        let cache_clone = Arc::clone(cache);

        thread::spawn(move || {
            let addr = SocketAddr::from((cache_clone.ipv4_addr, cache_clone.port));

            let listener = TcpListener::bind(addr)?;
            loop {
                let (socket, _addr) = listener.accept()?;
                let thread_cache = Arc::clone(&cache_clone);
                thread::spawn(move || (CacheDb::<KeyT, ValT>::client_handler(socket, &thread_cache)));

                #[cfg(test)]
                return Ok(());
            }
        })
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
                }
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
        let cache_client = CacheClient::<String, String>::create_connect([127, 0, 0, 1], 8080).unwrap();
        CacheClient::<String, String>::cache_client_handler(&cache_client);
        // .join().unwrap();

        let mut _test_res = KeyValObj{key: String::new(), val: String::new()};
        cache_client.pull(&"asd".to_string(), &mut _test_res).unwrap();

        assert_eq!(CacheDbError::KeyNotFound, cache_client.pull(&"non_existant".to_string(), &mut _test_res).unwrap_err());
    }

    #[test]
    fn basic_server_test() {
        let cache = CacheDb::<String, String>::new([127, 0, 0, 1], 8080);
        CacheDb::<String, String>::cache_db_server(&cache);
        // .join().unwrap();

        cache.push(KeyValObj{key: "asd".to_string(), val: "das".to_string()});
        thread::sleep(time::Duration::from_secs(1));
        basic_client_test();
    }

    #[test]
    fn local_cache_db_test() {
        let cache = CacheDb::<String, String>::new([127, 0, 0, 1], 8080);

        cache.push(KeyValObj {
            key: String::from("brian"),
            val: String::from("test"),
        });
        cache.push(KeyValObj {
            key: String::from("paul"),
            val: String::from("test1"),
        });
        cache.push(KeyValObj {
            key: String::from("pete"),
            val: String::from("test2"),
        });
        cache.push(KeyValObj {
            key: String::from("robert"),
            val: String::from("test3"),
        });

        let get_res = cache.get(&String::from("brian")).unwrap();
        assert_eq!(&get_res.val, "test");
        println!("get k: {} v: {}", get_res.key, get_res.val);

        let get_res = cache.get(&String::from("paul")).unwrap();
        assert_eq!(&get_res.val, "test1");
        println!("get k: {} v: {}", get_res.key, get_res.val);

        let get_res = cache.get(&String::from("pete")).unwrap();
        assert_eq!(&get_res.val, "test2");
        println!("get k: {} v: {}", get_res.key, get_res.val);

        let get_res = cache.get(&String::from("robert")).unwrap();
        assert_eq!(&get_res.val, "test3");
        println!("get k: {} v: {}", get_res.key, get_res.val);

        if let None = cache.get(&String::from("ian")) {
            println!("Ian not found!");
        } else {
            assert!(true);
        }

        if let Err(_e) = cache.set(String::from("robert"), String::from("mod_test")) {
            assert!(true);
        }
        let get_res = cache.get(&String::from("robert")).unwrap();
        assert_eq!(&get_res.val, "mod_test");
        println!("mod get k: {} v: {}", get_res.key, get_res.val);
    }
}
