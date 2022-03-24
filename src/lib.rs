use std::io::prelude::*;
use std::thread;
use std::net::{TcpStream, TcpListener};

fn client_handler(mut socket: TcpStream){
    let mut buf = [0; 10];
    socket.read(&mut buf);
    socket.write(&[1]);

    println!("read: {:?}", buf);
}

fn server_client_handler() {
    let listener = TcpListener::bind("127.0.0.1:1337").unwrap();
    let mut handlers = Vec::new();

    loop {
        match listener.accept() {
            Ok((socket, addr)) => {
                println!("new client: {:?}", addr);

                let handler = thread::spawn(move || {
                    client_handler(socket);
                });
                handlers.push(handler);
            }
            Err(e) => println!("couldn't get client: {:?}", e),
        }    
    }
}

fn client_process() {
    let mut stream = TcpStream::connect("127.0.0.1:1337").unwrap();
    stream.write(&[1]).unwrap();
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
