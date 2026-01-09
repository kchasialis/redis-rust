#![allow(unused_imports)]

mod resp_types;

use std::io::{Read, Write};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::io::Result;
use crate::resp_types::{RespValue};
use crate::resp_types::RespKey::SimpleString;

async fn handle_connection(mut stream: TcpStream) -> Result<()> {
   let mut buf = [0u8; 1024];
   loop {
       let _ = match stream.read(&mut buf).await {
           Ok(0) => return Ok(()),
           Ok(n) => {
               eprintln!("Received data in socket!");
               n
           }
           Err(e) => {
               eprintln!("failed to read from socket; err = {:?}", e);
               return Ok(())
           }
       };

       let val = RespValue::deserialize(&buf).unwrap();
       match &val {
           RespValue::Array(arr) => {
               match &arr[0] {
                   RespValue::BulkString(cmd) => {
                       let response;
                       if cmd == b"PING" {
                           eprintln!("Received PING command");
                           response = RespValue::SimpleString("PONG".to_string());
                       } else if cmd == b"ECHO" {
                           eprintln!("Received ECHO command");
                           response = arr[1].clone();
                       } else {
                           panic!("Received unsupported command")
                       }
                       stream.write_all(response.serialize().as_slice()).await?;
                   }
                   _ => panic!("Expected bulk string command")
               }
           }
           _ => panic!("Received unsupported value type")
       }
   }
}

#[tokio::main]
async fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        eprintln!("Waiting for new client...");
        let socket = listener.accept().await?.0;
        eprintln!("Accepted a client!");

        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket).await {
                eprintln!("connection error: {:?}", e);
            }
        });
    }
}
