#![allow(unused_imports)]

mod resp_types;

use std::io::{Read, Write};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::io::Result;
use crate::resp_types::{RespValue};
use crate::resp_types::RespKey::SimpleString;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

type Storage = Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>;

fn handle_ping_cmd() -> RespValue {
    eprintln!("Received PING command");
    RespValue::SimpleString("PONG".to_string())
}

fn handle_echo_cmd(args: &Vec<RespValue>) -> RespValue {
    eprintln!("Received ECHO command");
    args[1].clone()
}

async fn handle_set_cmd(args: &Vec<RespValue>, storage: Storage) -> RespValue {
    eprintln!("Received SET command");

    let key = match &args[1] {
        RespValue::BulkString(k) => k.clone(),
        _ => panic!("SET: expected bulk string type for key")
    };
    let value = match &args[2] {
        RespValue::BulkString(v) => v.clone(),
        _ => panic!("SET: expected bulk string type for value")
    };

    storage.write().await.insert(key, value);
    RespValue::SimpleString("OK".to_string())
}

async fn handle_get_cmd(args: &Vec<RespValue>, storage: Storage) -> RespValue {
    eprintln!("Received GET command");

    let key = match &args[1] {
        RespValue::BulkString(k) => k,
        _ => panic!("GET: expected bulk string type for key")
    };

    match storage.read().await.get(key) {
        Some(val) => RespValue::BulkString(val.clone()),
        None => RespValue::BulkString(b"$-1\r\n".to_vec())
    }
}

async fn handle_connection(mut stream: TcpStream, storage: Storage) -> Result<()> {
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
                           response = handle_ping_cmd();
                       } else if cmd == b"ECHO" {
                           response = handle_echo_cmd(arr);
                       } else if cmd == b"SET" {
                           response = handle_set_cmd(arr, storage.clone()).await;
                       } else if cmd == b"GET" {
                           response = handle_get_cmd(arr, storage.clone()).await;
                       } else {
                           panic!("Received unsupported command")
                       }
                       stream.write_all(response.serialize().as_slice()).await?;
                   }
                   _ => panic!("Expected bulk string as array element in command")
               }
           }
           _ => panic!("Expected array type for command")
       }
   }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Logs from your program will appear here!");

    let storage: Storage = Arc::new(RwLock::new(HashMap::new()));

    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        eprintln!("Waiting for new client...");
        let socket = listener.accept().await?.0;
        eprintln!("Accepted a client!");

        let storage_clone = Arc::clone(&storage);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, storage_clone).await {
                eprintln!("connection error: {:?}", e);
            }
        });
    }
}
