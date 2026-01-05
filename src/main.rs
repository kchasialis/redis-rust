#![allow(unused_imports)]

use std::io::{Read, Write};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::io::Result;

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

       eprintln!("Sending pong!");

       stream.write_all(b"+PONG\r\n").await?;
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
