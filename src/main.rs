#![allow(unused_imports)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::io::Result;

fn handle_connection(mut stream: TcpStream) -> Result<()> {
   loop {
       let mut buf = [0u8; 4096];
       let n = stream.read(&mut buf)?;
       if n == 0 {
           return Ok(());
       }
       stream.write_all(b"+PONG\r\n")?;
   }
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_connection(stream).unwrap()
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
