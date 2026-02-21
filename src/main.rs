#![allow(unused_imports)]

mod resp_types;
mod storage;
mod task_communication;

use storage::Storage;

use std::io::{Read, Write};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::io::Result;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::resp_types::{RespKey, RespValue, StreamId};
use crate::resp_types::RespKey::{BulkString, SimpleString};
use crate::storage::StorageValue;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::time;
use tokio::time::timeout;
use std::ops::Bound;
use crate::task_communication::{Channels, WaiterRegistry};

fn handle_ping_cmd() -> RespValue {
    eprintln!("Received PING command");
    RespValue::SimpleString("PONG".to_string())
}

fn handle_echo_cmd(args: &Vec<RespValue>) -> RespValue {
    eprintln!("Received ECHO command");
    args[1].clone()
}

fn parse_int_from_bulk_str(resp_val: &RespValue) -> i64 {
    match resp_val {
        RespValue::BulkString(v) => {
            std::str::from_utf8(v)
                .expect("Error in parsing int from resp_value (bulk_str): Invalid UTF-8")
                .parse::<i64>()
                .expect("Error in parsing int from resp_value (bulk_str): Invalid number")
        }
        _ => panic!("Expected bulk string type for command argument")
    }
}

fn parse_float_from_bulk_str(resp_val: &RespValue) -> f64 {
    match resp_val {
        RespValue::BulkString(v) => {
            std::str::from_utf8(v)
                .expect("Error in parsing int from resp_value (bulk_str): Invalid UTF-8")
                .parse::<f64>()
                .expect("Error in parsing int from resp_value (bulk_str): Invalid number")
        }
        _ => panic!("Expected bulk string type for command argument")
    }
}

fn parse_str_from_bulk_str(resp_val: &RespValue) -> String {
    match resp_val {
        RespValue::BulkString(v) => {
            std::str::from_utf8(v).unwrap().to_lowercase()
        }
        _ => panic!("Expected bulk string type for command argument")
    }
}

async fn handle_set_cmd(args: &Vec<RespValue>, storage: Storage) -> RespValue {
    let mut expiry_duration: Option<Duration> = None;
    if args.len() == 5 {
        let expiry_type = match &args[3] {
            RespValue::BulkString(v) => v,
            _ => panic!("SET: expected bulk string type for expiry type")
        };
        let expiry_value = parse_int_from_bulk_str(&args[4]) as u64;

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

async fn handle_rpush_cmd(args: &Vec<RespValue>, storage: Storage, channels: Channels) -> RespValue {
    let key = RespKey::from(args[1].clone());

    let mut guard = storage.write().await;

    let list_len = match guard.get_mut(&key) {
        Some(storage_val) => {
            match storage_val.data_mut() {
                Some(RespValue::Array(vec)) => {
                    for arg in &args[2..] {
                        vec.push_back(arg.clone());
                    }
                    vec.len()
                }
                Some(_) => panic!("RPUSH: key exists but is not an array"),
                None => {
                    let mut list = VecDeque::new();
                    for arg in &args[2..] {
                        list.push_back(arg.clone());
                    }
                    let len = list.len();
                    *storage_val = StorageValue::new(RespValue::Array(list), None);
                    len
                }
            }
        }
        None => {
            let mut list = VecDeque::new();
            for arg in &args[2..] {
                list.push_back(arg.clone());
            }
            let len = list.len();
            guard.insert(key.clone(), StorageValue::new(RespValue::Array(list), None));
            len
        }
    };

    if let Some(sender) = channels.write().await.get_mut(&key).and_then(|reg| reg.pop_waiter()) {
        if let Some(storage_val) = guard.get_mut(&key) {
            if let Some(RespValue::Array(vec)) = storage_val.data_mut() {
                if let Some(popped) = vec.pop_front() {
                    let mut vec = VecDeque::new();
                    vec.push_back(args[1].clone());
                    vec.push_back(popped);
                    let response = RespValue::Array(vec);
                    sender.send(response).await.ok();
                }
            }
        }
    }

    RespValue::Integer(list_len as i64)
}

async fn handle_lpush_cmd(args: &Vec<RespValue>, storage: Storage) -> RespValue {
    let key = RespKey::from(args[1].clone());

    let mut guard = storage.write().await;

    let list_len = match guard.get_mut(&key) {
        Some(storage_val) => {
            match storage_val.data_mut() {
                Some(RespValue::Array(vec)) => {
                    for arg in &args[2..] {
                        vec.push_front(arg.clone());
                    }
                    vec.len()
                }
                Some(_) => panic!("LPUSH: key exists but is not an array"),
                None => {
                    let mut list = VecDeque::new();
                    for arg in &args[2..] {
                        list.push_front(arg.clone());
                    }
                    let len = list.len();
                    *storage_val = StorageValue::new(RespValue::Array(list), None);
                    len
                }
            }
        }
        None => {
            let mut list = VecDeque::new();
            for arg in &args[2..] {
                list.push_front(arg.clone());
            }
            let len = list.len();
            guard.insert(key, StorageValue::new(RespValue::Array(list), None));
            len
        }
    };

    RespValue::Integer(list_len as i64)
}

async fn handle_lpop_cmd(args: &Vec<RespValue>, storage: Storage) -> RespValue {
    let key = RespKey::from(args[1].clone());

    let mut guard = storage.write().await;

    match guard.get_mut(&key) {
        Some(storage_val) => {
            match storage_val.data_mut() {
                Some(RespValue::Array(vec)) => {
                    if args.len() == 3 {
                        let mut elems_to_remove = parse_int_from_bulk_str(&args[2]) as usize;
                        if elems_to_remove >= vec.len() {
                            elems_to_remove = vec.len();
                        }

                        let mut result = VecDeque::new();
                        for _ in 0..elems_to_remove {
                            let val = vec[0].clone();
                            vec.pop_front();
                            result.push_back(val);
                        }
                        RespValue::Array(result)
                    } else {
                        if vec.len() == 0 {
                            return RespValue::NullBulkString;
                        }
                        let val = vec[0].clone();
                        vec.pop_front();
                        val
                    }
                }
                Some(_) => panic!("LPOP: key exists but is not an array"),
                None => {
                    RespValue::NullBulkString
                }
            }
        }
        None => {
            RespValue::NullBulkString
        }
    }
}

async fn handle_blpop_cmd(args: &Vec<RespValue>, storage: Storage, channels: Channels) -> RespValue {
    let key = RespKey::from(args[1].clone());
    let timeout_secs = parse_float_from_bulk_str(&args[2]);

    {
        let mut guard = storage.write().await;
        if let Some(storage_val) = guard.get_mut(&key) {
            if let Some(RespValue::Array(deque)) = storage_val.data_mut() {
                if !deque.is_empty() {
                    let popped = deque.pop_front().unwrap();
                    let mut vec = VecDeque::new();
                    vec.push_back(args[1].clone());
                    vec.push_back(popped);
                    return RespValue::Array(vec);
                }
            }
        }
    }

    let (tx, mut rx) = mpsc::channel(1);

    channels.write().await
        .entry(key)
        .or_insert_with(|| WaiterRegistry { senders: VecDeque::new() })
        .add_waiter(tx);

    if timeout_secs == 0.0 {
        rx.recv().await.unwrap_or(RespValue::NullArray)
    } else {
        match timeout(Duration::from_secs_f64(timeout_secs), rx.recv()).await {
            Ok(Some(response)) => response,
            Ok(None) | Err(_) => RespValue::NullArray,
        }
    }
}

async fn handle_lrange_cmd(args: &Vec<RespValue>, storage: Storage) -> RespValue {
    let key = RespKey::from(args[1].clone());

    let start = parse_int_from_bulk_str(&args[2]);
    let stop = parse_int_from_bulk_str(&args[3]);

    let value_opt = storage.read().await.get(&key)
        .and_then(|val| val.data()).cloned();

    match value_opt {
        Some(RespValue::Array(vec)) => {
            let len = vec.len() as i64;

            let start_idx = if start < 0 {
                (len + start).max(0)
            } else {
                start.min(len)
            } as usize;

            let stop_idx = if stop < 0 {
                (len + stop + 1).max(0)
            } else {
                (stop + 1).min(len)
            } as usize;

            if start_idx >= stop_idx || start_idx >= vec.len() {
                return RespValue::Array(VecDeque::new());
            }

            let mut result = VecDeque::new();
            for i in start_idx..stop_idx {
                result.push_back(vec[i].clone());
            }
            RespValue::Array(result)
        }
        Some(_) => panic!("LRANGE: key exists but is not an array"),
        None => {
            storage.write().await.remove(&key);
            RespValue::Array(VecDeque::new())
        }
    }
}

async fn handle_llen_cmd(args: &Vec<RespValue>, storage: Storage) -> RespValue {
    let key = RespKey::from(args[1].clone());

    let value_opt = storage.read().await.get(&key)
        .and_then(|val| val.data()).cloned();

    match value_opt {
        Some(RespValue::Array(vec)) => {
            RespValue::Integer(vec.len() as i64)
        }
        Some(_) => panic!("LRANGE: key exists but is not an array"),
        None => {
            storage.write().await.remove(&key);
            RespValue::Integer(0)
        }
    }
}

async fn handle_type_cmd(args: &Vec<RespValue>, storage: Storage) -> RespValue {
    let key = RespKey::from(args[1].clone());

    let value_opt = storage.read().await.get(&key)
        .and_then(|val| val.data()).cloned();

    match value_opt {
        Some(RespValue::SimpleString(_)) | Some(RespValue::BulkString(_)) => {
            RespValue::SimpleString("string".to_string())
        }
        Some(RespValue::Stream(_)) => {
            RespValue::SimpleString("stream".to_string())
        }
        Some(_) => panic!("LRANGE: key exists but is not an array"),
        None => {
            storage.write().await.remove(&key);
            RespValue::SimpleString("none".to_string())
        }
    }
}

async fn handle_xadd_cmd(args: &Vec<RespValue>, storage: Storage, channels: Channels) -> RespValue {
    let key = RespKey::from(args[1].clone());
    let mut stream_id = StreamId::from(args[2].clone());

    if stream_id.sequence == Some(0) && stream_id.milliseconds == Some(0) {
        return RespValue::SimpleError(
            "ERR The ID specified in XADD must be greater than 0-0".to_string()
        )
    }

    if stream_id.milliseconds.is_none() {
        stream_id.milliseconds = Some(SystemTime::now()
                                          .duration_since(UNIX_EPOCH)
                                          .unwrap()
                                          .as_millis() as u64);
    }

    {
        let mut guard = storage.write().await;

        match guard.get_mut(&key) {
            Some(storage_val) => {
                match storage_val.data_mut() {
                    Some(RespValue::Stream(map)) => {
                        let same_ms_last = map.range(
                            StreamId { milliseconds: stream_id.milliseconds, sequence: Some(0) }..=
                                StreamId { milliseconds: stream_id.milliseconds, sequence: Some(u64::MAX) }
                        ).next_back();

                        if stream_id.sequence.is_none() {
                            stream_id.sequence = match same_ms_last {
                                Some((last_id, _)) => Some(last_id.sequence.unwrap() + 1),
                                None => {
                                    if stream_id.milliseconds == Some(0) { Some(1) } else { Some(0) }
                                }
                            };
                        }

                        if let Some(last_id) = map.keys().next_back() {
                            if stream_id <= *last_id {
                                return RespValue::SimpleError(
                                    "ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string()
                                );
                            }
                        }

                        let entry = map.entry(stream_id.clone()).or_insert_with(HashMap::new);
                        let mut i = 3;
                        while i < args.len() - 1 {
                            entry.insert(RespKey::from(args[i].clone()), args[i + 1].clone());
                            i += 2;
                        }
                    }
                    Some(_) => panic!("XADD: key exists but is not a stream"),
                    None => panic!("XADD: key expired")
                }
            }
            None => {
                let mut map = BTreeMap::new();
                let mut hashmap = HashMap::new();
                let mut i = 3;
                while i < args.len() - 1 {
                    hashmap.insert(RespKey::from(args[i].clone()), args[i + 1].clone());
                    i += 2;
                }

                if stream_id.sequence.is_none() {
                    stream_id.sequence = if stream_id.milliseconds == Some(0) { Some(1) } else { Some(0) }
                }
                map.insert(stream_id.clone(), hashmap);
                guard.insert(key.clone(), StorageValue::new(RespValue::Stream(map), None));
            }
        }
    }

    if let Some(sender) = channels.write().await.get_mut(&key).and_then(|reg| reg.pop_waiter()) {
        sender.send(args[1].clone()).await.ok();
    }

    RespValue::BulkString(format!("{}-{}", stream_id.milliseconds.unwrap(), stream_id.sequence.unwrap()).into())
}

fn build_entries_vec<'a>(range: impl Iterator<Item = (&'a StreamId, &'a HashMap<RespKey, RespValue>)>) -> VecDeque<RespValue> {
    let mut entries_vec = VecDeque::new();
    for (sid, hashmap) in range {
        let mut inner_vec = VecDeque::new();
        for entry in hashmap {
            inner_vec.push_back(RespValue::from(entry.0.clone()));
            inner_vec.push_back(entry.1.clone());
        }
        let mut entry_vec = VecDeque::new();
        entry_vec.push_back(RespValue::BulkString(sid.to_string().into_bytes()));
        entry_vec.push_back(RespValue::Array(inner_vec));
        entries_vec.push_back(RespValue::Array(entry_vec));
    }
    entries_vec
}

async fn handle_xrange_cmd(args: &Vec<RespValue>, storage: Storage) -> RespValue {
    let key = RespKey::from(args[1].clone());
    let mut start_id = StreamId::from(args[2].clone());
    let mut end_id = StreamId::from(args[3].clone());

    if start_id.sequence.is_none() {
        start_id.sequence = Some(0);
        if start_id.milliseconds.is_none() {
            start_id.milliseconds = Some(0);
        }
    }
    if end_id.sequence.is_none() {
        end_id.sequence = Some(u64::MAX);
        if end_id.milliseconds.is_none() {
            end_id.milliseconds = Some(u64::MAX);
        }
    }

    let value_opt = storage.read().await.get(&key)
        .and_then(|val| val.data()).cloned();

    match value_opt {
        Some(RespValue::Stream(map)) => {
            RespValue::Array(build_entries_vec(map.range(start_id..=end_id)))
        }
        Some(_) => panic!("XRANGE: key exists but is not a stream"),
        None => {
            storage.write().await.remove(&key);
            RespValue::Array(VecDeque::new())
        }
    }
}

async fn handle_xread_cmd(args: &Vec<RespValue>, storage: Storage, channels: Channels) -> RespValue {
    let is_blocking = parse_str_from_bulk_str(&args[1]).eq("block");

    let key_start_idx = if is_blocking { 4 } else { 2 };
    let n_keys = (args.len() - key_start_idx) / 2;
    let id_start_idx = key_start_idx + n_keys;

    let ids: Vec<StreamId> = {
        let guard = storage.read().await;
        (0..n_keys).map(|i| {
            let id_str = parse_str_from_bulk_str(&args[id_start_idx + i]);
            if id_str.eq("$") {
                let key = RespKey::from(args[key_start_idx + i].clone());
                guard.get(&key)
                    .and_then(|val| val.data())
                    .and_then(|v| if let RespValue::Stream(map) = v { map.keys().next_back().cloned() } else { None })
                    .unwrap_or(StreamId { milliseconds: Some(0), sequence: Some(0) })
            } else {
                StreamId::from(args[id_start_idx + i].clone())
            }
        }).collect()
    };

    if is_blocking {
        let timeout_ms = parse_int_from_bulk_str(&args[2]) as u64;
        let key = RespKey::from(args[key_start_idx].clone());
        let (tx, mut rx) = mpsc::channel(1);

        channels.write().await
            .entry(key)
            .or_insert_with(|| WaiterRegistry { senders: VecDeque::new() })
            .add_waiter(tx);

        let timed_out = if timeout_ms == 0 {
            rx.recv().await.is_none()
        } else {
            match timeout(Duration::from_millis(timeout_ms), rx.recv()).await {
                Ok(Some(_)) => false,
                Ok(None) | Err(_) => true,
            }
        };

        if timed_out {
            return RespValue::NullArray;
        }
    }

    let guard = storage.read().await;
    let mut streams_vec = VecDeque::new();

    for i in 0..n_keys {
        let key = RespKey::from(args[key_start_idx + i].clone());

        match guard.get(&key).and_then(|val| val.data()) {
            Some(RespValue::Stream(map)) => {
                let entries = build_entries_vec(map.range((Excluded(&ids[i]), Unbounded)));
                let mut stream_vec = VecDeque::new();
                stream_vec.push_back(args[key_start_idx + i].clone());
                stream_vec.push_back(RespValue::Array(entries));
                streams_vec.push_back(RespValue::Array(stream_vec));
            }
            Some(_) => panic!("XREAD: key exists but is not a stream"),
            None => streams_vec.push_back(RespValue::NullArray),
        }
    }

    RespValue::Array(streams_vec)
}

async fn handle_connection(mut stream: TcpStream, storage: Storage, channels: Channels) -> Result<()> {
   let mut buf = [0u8; 1024];
   loop {
       let _ = match stream.read(&mut buf).await {
           Ok(0) => return Ok(()),
           Ok(n) => {
               n
           }
           Err(e) => {
               eprintln!("failed to read from socket; err = {:?}", e);
               return Ok(())
           }
       };

       let val = RespValue::deserialize(&buf).unwrap();
       match &val {
           RespValue::Array(deque) => {
               let arr: Vec<RespValue> = deque.iter().cloned().collect();
               match &arr[0] {
                   RespValue::BulkString(cmd) => {
                       let response;
                       if cmd == b"PING" {
                           response = handle_ping_cmd();
                       } else if cmd == b"ECHO" {
                           response = handle_echo_cmd(&arr);
                       } else if cmd == b"SET" {
                           response = handle_set_cmd(&arr, storage.clone()).await;
                       } else if cmd == b"GET" {
                           response = handle_get_cmd(&arr, storage.clone()).await;
                       } else if cmd == b"RPUSH" {
                           response = handle_rpush_cmd(&arr, storage.clone(), channels.clone()).await;
                       } else if cmd == b"LPUSH" {
                           response = handle_lpush_cmd(&arr, storage.clone()).await;
                       } else if cmd == b"LPOP" {
                           response = handle_lpop_cmd(&arr, storage.clone()).await;
                       } else if cmd == b"BLPOP" {
                           response = handle_blpop_cmd(&arr, storage.clone(), channels.clone()).await;
                       } else if cmd == b"LRANGE" {
                           response = handle_lrange_cmd(&arr, storage.clone()).await;
                       } else if cmd == b"LLEN" {
                           response = handle_llen_cmd(&arr, storage.clone()).await;
                       } else if cmd == b"TYPE" {
                           response = handle_type_cmd(&arr, storage.clone()).await;
                       } else if cmd == b"XADD" {
                           response = handle_xadd_cmd(&arr, storage.clone(), channels.clone()).await;
                       } else if cmd == b"XRANGE" {
                           response = handle_xrange_cmd(&arr, storage.clone()).await;
                       } else if cmd == b"XREAD" {
                           response = handle_xread_cmd(&arr, storage.clone(), channels.clone()).await;
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
    let channels: Channels = Arc::new(RwLock::new(HashMap::new()));

    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        let socket = listener.accept().await?.0;

        let storage_clone = Arc::clone(&storage);
        let channels_clone = Arc::clone(&channels);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, storage_clone, channels_clone).await {
                eprintln!("connection error: {:?}", e);
            }
        });
    }
}
