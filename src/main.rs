#![allow(unused_imports)]

mod resp_types;
mod storage;
use storage::Storage;

use std::io::{Read, Write};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::io::Result;
use std::time::Duration;
use crate::resp_types::{RespKey, RespValue};
use crate::resp_types::RespKey::SimpleString;
use crate::storage::StorageValue;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

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

    let mut expiry_duration: Option<Duration> = None;
    if args.len() == 5 {
        let expiry_type = match &args[3] {
            RespValue::BulkString(v) => v,
            _ => panic!("SET: expected bulk string type for expiry type")
        };
        let expiry_value = match &args[4] {
            RespValue::BulkString(v) => {
                std::str::from_utf8(v)
                    .expect("SET: invalid UTF-8 in expiry value")
                    .parse::<u64>()
                    .expect("SET: invalid number for expiry value")
            }
            _ => panic!("SET: expected bulk string type for expiry value")
        };

        eprintln!("[DEBUG] Received expiry_type: {:?}, expiry_value: {}", String::from_utf8(expiry_type.clone()), expiry_value);

        if expiry_type.eq(b"EX") {
            expiry_duration = Some(Duration::from_secs(expiry_value));
        } else if expiry_type.eq(b"PX") {
            expiry_duration = Some(Duration::from_millis(expiry_value));
        } else {
            panic!("SET: unhandled specifier for expiry time type")
        }
    }

    storage.write().await.insert(args[1].clone().into(),
                                 StorageValue::new(args[2].clone(), expiry_duration));
    RespValue::SimpleString("OK".to_string())
}

async fn handle_get_cmd(args: &Vec<RespValue>, storage: Storage) -> RespValue {
    eprintln!("[DEBUG] Received GET command");

    let key = RespKey::from(args[1].clone());

    let value_opt = storage.read().await.get(&key)
        .and_then(|val| val.data()).cloned();

    match value_opt {
        Some(val) => val,
        None => {
            storage.write().await.remove(&key);
            RespValue::NullBulkString
        }
    }
}

async fn handle_rpush_cmd(args: &Vec<RespValue>, storage: Storage) -> RespValue {
    let key = RespKey::from(args[1].clone());

    let mut guard = storage.write().await;

    let list_len = match guard.get_mut(&key) {
        Some(storage_val) => {
            match storage_val.data_mut() {
                Some(RespValue::Array(vec)) => {
                    for arg in &args[2..] {
                        vec.push(arg.clone());
                    }
                    vec.len()
                }
                Some(_) => panic!("RPUSH: key exists but is not an array"),
                None => {
                    let mut list = Vec::new();
                    for arg in &args[2..] {
                        list.push(arg.clone());
                    }
                    let len = list.len();
                    *storage_val = StorageValue::new(RespValue::Array(list), None);
                    len
                }
            }
        }
        None => {
            let mut list = Vec::new();
            for arg in &args[2..] {
                list.push(arg.clone());
            }
            let len = list.len();
            guard.insert(key, StorageValue::new(RespValue::Array(list), None));
            len
        }
    };

    RespValue::Integer(list_len as i64)
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
                       } else if cmd == b"RPUSH" {
                           response = handle_rpush_cmd(arr, storage.clone()).await;
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
