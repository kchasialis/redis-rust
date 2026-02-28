pub mod resp_types;
pub mod storage;
pub mod task_communication;
pub mod replication;

use storage::Storage;

use tokio::net::{TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::resp_types::{parse_resp_bytes, RespKey, RespValue, StreamId};
use crate::storage::StorageValue;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Excluded, Unbounded};
use tokio::sync::mpsc;
use std::collections::VecDeque;
use clap::Parser;
use tokio::time::timeout;
use crate::replication::{NodeRole, ReplicaConnection, ReplicaInfo, ReplicationStateHandle};
use crate::task_communication::{Channels, WaiterRegistry};

#[derive(Parser, Debug)]
#[command(name = "redis-server", about = "A Redis-like server")]
pub struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    #[arg(long, default_value_t = 6379)]
    pub port: u16,

    #[arg(long)]
    pub replicaof: Option<String>
}

pub fn handle_ping_cmd() -> RespValue {
    eprintln!("Received PING command");
    RespValue::SimpleString("PONG".to_string())
}

pub fn handle_echo_cmd(args: &Vec<RespValue>) -> RespValue {
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

pub async fn handle_set_cmd(args: &Vec<RespValue>, storage: Storage, repl_state: ReplicationStateHandle) -> RespValue {
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

    let mut state = repl_state.write().await;
    match state.role {
        NodeRole::Master { replicas: ref mut replicas_write } => {
            let mut resp_vec = VecDeque::new();
            for arg in args {
                resp_vec.push_back(arg.clone());
            }
            let cmd_bytes = RespValue::Array(resp_vec).serialize();
            for conn in replicas_write {
                conn.write.write_all(cmd_bytes.clone().as_slice()).await.expect("Failed to send SET to replica");
            }

            state.repl_offset += cmd_bytes.len() as u64;
        }
        NodeRole::Replica { info: _ } => {}
    }

    RespValue::SimpleString("OK".to_string())
}

pub async fn handle_get_cmd(args: &Vec<RespValue>, storage: Storage) -> RespValue {
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

pub async fn handle_rpush_cmd(args: &Vec<RespValue>, storage: Storage, channels: Channels) -> RespValue {
    let key = RespKey::from(args[1].clone());

    let list_len = {
        let mut guard = storage.write().await;
        match guard.get_mut(&key) {
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
        }
    };

    if let Some(sender) = channels.write().await.get_mut(&key).and_then(|reg| reg.pop_waiter()) {
        let mut guard = storage.write().await;
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

pub async fn handle_lpush_cmd(args: &Vec<RespValue>, storage: Storage) -> RespValue {
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

pub async fn handle_lpop_cmd(args: &Vec<RespValue>, storage: Storage) -> RespValue {
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

pub async fn handle_blpop_cmd(args: &Vec<RespValue>, storage: Storage, channels: Channels) -> RespValue {
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

pub async fn handle_lrange_cmd(args: &Vec<RespValue>, storage: Storage) -> RespValue {
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

pub async fn handle_llen_cmd(args: &Vec<RespValue>, storage: Storage) -> RespValue {
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

pub async fn handle_type_cmd(args: &Vec<RespValue>, storage: Storage) -> RespValue {
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

pub async fn handle_xadd_cmd(args: &Vec<RespValue>, storage: Storage, channels: Channels) -> RespValue {
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

pub async fn handle_xrange_cmd(args: &Vec<RespValue>, storage: Storage) -> RespValue {
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

pub async fn handle_xread_cmd(args: &Vec<RespValue>, storage: Storage, channels: Channels) -> RespValue {
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

pub async fn handle_incr_cmd(args: &Vec<RespValue>, storage: Storage) -> RespValue {
    let key = RespKey::from(args[1].clone());
    let mut guard = storage.write().await;

    let current_val: i64 = match guard.get(&key) {
        Some(storage_val) => {
            match storage_val.data() {
                Some(RespValue::BulkString(v)) => {
                    match std::str::from_utf8(v).ok().and_then(|s| s.parse::<i64>().ok()) {
                        Some(n) => n,
                        None => return RespValue::SimpleError(
                            "ERR value is not an integer or out of range".to_string()
                        ),
                    }
                }
                Some(_) => return RespValue::SimpleError(
                    "ERR value is not an integer or out of range".to_string()
                ),
                None => 0,
            }
        }
        None => 0,
    };

    let new_val = current_val + 1;
    guard.insert(key, StorageValue::new(
        RespValue::BulkString(new_val.to_string().into_bytes()),
        None,
    ));
    RespValue::Integer(new_val)
}

pub async fn handle_info_cmd(_args: &Vec<RespValue>, _storage: Storage, repl_state: ReplicationStateHandle) -> RespValue {
    let guard = repl_state.read().await;

    let result = match guard.role {
        NodeRole::Master { replicas: _ } => {
            format!("role:master\nmaster_replid:{}\nmaster_repl_offset:{}", guard.repl_id, guard.repl_offset).into_bytes()
        }
        NodeRole::Replica { info: _ } => {
            format!("role:slave\nmaster_replid:{}\nmaster_repl_offset:{}", guard.repl_id, guard.repl_offset).into_bytes()
        }
    };

    RespValue::BulkString(result)
}

pub async fn handle_replconf_cmd(args: &Vec<RespValue>, _storage: Storage, repl_state: ReplicationStateHandle) -> RespValue {
    let arg = parse_str_from_bulk_str(&args[1]);

    if arg.eq("getack") {
        let mut result_vec = VecDeque::new();
        result_vec.push_back(RespValue::BulkString(b"REPLCONF".to_vec()));
        result_vec.push_back(RespValue::BulkString(b"ACK".to_vec()));
        let offset = repl_state.read().await.repl_offset;
        result_vec.push_back(RespValue::BulkString(offset.to_string().into_bytes()));

        RespValue::Array(result_vec)
    } else {
        RespValue::SimpleString("OK".to_string())
    }
}

pub async fn handle_psync_cmd(_args: &Vec<RespValue>, _storage: Storage, repl_state: ReplicationStateHandle) -> RespValue {
    let guard = repl_state.read().await;

    let result = match guard.role {
        NodeRole::Master { replicas: _ } => {
            format!("FULLRESYNC {} {}", guard.repl_id, guard.repl_offset)
        }
        NodeRole::Replica { info: _ } => {
            panic!("Replica should not receive psync command")
        }
    };

    RespValue::SimpleString(result)
}

pub async fn handle_exec_cmd(storage: Storage, channels: Channels, command_queue: &mut VecDeque<Vec<RespValue>>, repl_state: ReplicationStateHandle) -> RespValue {
    let mut result_vec = VecDeque::new();

    for command in command_queue.iter() {
        let resp = handle_cmd(command, storage.clone(), channels.clone(), repl_state.clone()).await;
        result_vec.push_back(resp);
    }

    command_queue.clear();
    RespValue::Array(result_vec)
}

pub async fn handle_wait_cmd(arr: &Vec<RespValue>, repl_state: ReplicationStateHandle) -> RespValue {
    let timeout = parse_int_from_bulk_str(&arr[2]);

    let replconf_cmd_bytes = RespValue::Array(VecDeque::from([
        RespValue::BulkString(b"REPLCONF".to_vec()),
        RespValue::BulkString(b"GETACK".to_vec()),
        RespValue::BulkString(b"*".to_vec()),
    ])).serialize();

    let mut guard = repl_state.write().await;
    let repl_offset = guard.repl_offset;

    let n_replicas = match guard.role {
        NodeRole::Master { ref mut replicas } => {
            if repl_offset == 0 {
                replicas.len()
            } else {
                for replica in replicas.iter_mut() {
                    replica.write.write_all(replconf_cmd_bytes.as_slice()).await.expect("Failed to send REPLCONF to replica");
                }

                let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout as u64);

                let mut n_replicas = 0;
                for replica in replicas.iter_mut() {
                    let mut buf = [0u8; 1024];
                    match tokio::time::timeout_at(deadline, replica.read.read(&mut buf)).await {
                        Ok(Ok(n)) if n > 0 => {
                            let val = RespValue::deserialize(&buf).unwrap();
                            let RespValue::Array(deque) = &val else {
                                panic!("Expected array type for command");
                            };
                            let repl_offset = parse_int_from_bulk_str(deque.back().unwrap());
                            if repl_offset >= repl_offset {
                                n_replicas += 1;
                            }
                        }
                        _ => {}
                    }
                }

                n_replicas as usize
            }
        },
        NodeRole::Replica { .. } => {
            panic!("Replicas should not receive this command")
        }
    };

    RespValue::Integer(n_replicas as i64)
}

pub async fn handle_cmd(arr: &Vec<RespValue>, storage: Storage, channels: Channels, repl_state: ReplicationStateHandle) -> RespValue {
    match &arr[0] {
        RespValue::BulkString(cmd) => {
            if cmd == b"PING" {
                handle_ping_cmd()
            } else if cmd == b"ECHO" {
                handle_echo_cmd(arr)
            } else if cmd == b"SET" {
                handle_set_cmd(arr, storage, repl_state).await
            } else if cmd == b"GET" {
                handle_get_cmd(arr, storage).await
            } else if cmd == b"RPUSH" {
                handle_rpush_cmd(arr, storage, channels).await
            } else if cmd == b"LPUSH" {
                handle_lpush_cmd(arr, storage).await
            } else if cmd == b"LPOP" {
                handle_lpop_cmd(arr, storage).await
            } else if cmd == b"BLPOP" {
                handle_blpop_cmd(arr, storage, channels).await
            } else if cmd == b"LRANGE" {
                handle_lrange_cmd(arr, storage).await
            } else if cmd == b"LLEN" {
                handle_llen_cmd(arr, storage).await
            } else if cmd == b"TYPE" {
                handle_type_cmd(arr, storage).await
            } else if cmd == b"XADD" {
                handle_xadd_cmd(arr, storage, channels).await
            } else if cmd == b"XRANGE" {
                handle_xrange_cmd(arr, storage).await
            } else if cmd == b"XREAD" {
                handle_xread_cmd(arr, storage, channels).await
            } else if cmd == b"INCR" {
                handle_incr_cmd(arr, storage).await
            } else if cmd == b"INFO" {
                handle_info_cmd(arr, storage, repl_state).await
            } else if cmd == b"REPLCONF" {
                handle_replconf_cmd(arr, storage, repl_state).await
            } else if cmd == b"PSYNC" {
                handle_psync_cmd(arr, storage, repl_state).await
            } else if cmd == b"WAIT" {
                handle_wait_cmd(arr, repl_state).await
            } else {
                RespValue::SimpleError(format!(
                    "ERR unknown command '{}'",
                    String::from_utf8_lossy(cmd)
                ))
            }
        }
        _ => panic!("Expected bulk string as first element in command array"),
    }
}

pub async fn handle_connection(mut stream: TcpStream, storage: Storage, channels: Channels, repl_state: ReplicationStateHandle) -> std::io::Result<()> {
    let mut in_transaction = false;
    let mut command_queue: VecDeque<Vec<RespValue>> = VecDeque::new();

    let mut buf = [0u8; 1024];
    loop {
        match stream.read(&mut buf).await {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(e) => {
                eprintln!("failed to read from socket; err = {:?}", e);
                return Ok(());
            }
        };

        let val = RespValue::deserialize(&buf).unwrap();
        let RespValue::Array(deque) = &val else {
            panic!("Expected array type for command");
        };

        let arr: Vec<RespValue> = deque.iter().cloned().collect();
        let RespValue::BulkString(cmd) = &arr[0] else {
            panic!("Expected bulk string as array element in command");
        };

        let response = if cmd == b"MULTI" {
            in_transaction = true;
            RespValue::SimpleString("OK".to_string())
        } else if cmd == b"EXEC" {
            if !in_transaction {
                RespValue::SimpleError("ERR EXEC without MULTI".to_string())
            } else {
                in_transaction = false;
                handle_exec_cmd(storage.clone(), channels.clone(), &mut command_queue, repl_state.clone()).await
            }
        } else if cmd == b"DISCARD" {
            if !in_transaction {
                RespValue::SimpleError("ERR DISCARD without MULTI".to_string())
            } else {
                in_transaction = false;
                command_queue.clear();
                RespValue::SimpleString("OK".to_string())
            }
        } else if in_transaction {
            command_queue.push_back(arr.clone());
            RespValue::SimpleString("QUEUED".to_string())
        } else {
            handle_cmd(&arr, storage.clone(), channels.clone(), repl_state.clone()).await
        };

        stream.write_all(response.serialize().as_slice()).await?;

        if cmd == b"PSYNC" {
            let empty_rdb = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2").unwrap();
            let header = format!("${}\r\n", empty_rdb.len());
            stream.write_all(header.as_bytes()).await?;
            stream.write_all(&empty_rdb).await?;

            let (read_half, write_half) = stream.into_split();
            repl_state.write().await.add_replica(ReplicaConnection { write: write_half, read: read_half });
            return Ok(());
        }
    }
}

pub async fn run_replication(replica_info: ReplicaInfo, port: u16, storage: Storage, channels: Channels, repl_state: ReplicationStateHandle) {
    let mut stream = TcpStream::connect(format!("{}:{}", replica_info.m_host, replica_info.m_port))
        .await
        .expect("Failed to connect to master");

    let commands: Vec<RespValue> = vec![
        RespValue::Array(VecDeque::from([
            RespValue::BulkString(b"PING".to_vec()),
        ])),
        RespValue::Array(VecDeque::from([
            RespValue::BulkString(b"REPLCONF".to_vec()),
            RespValue::BulkString(b"listening-port".to_vec()),
            RespValue::BulkString(port.to_string().into_bytes()),
        ])),
        RespValue::Array(VecDeque::from([
            RespValue::BulkString(b"REPLCONF".to_vec()),
            RespValue::BulkString(b"capa".to_vec()),
            RespValue::BulkString(b"psync2".to_vec()),
        ])),
        RespValue::Array(VecDeque::from([
            RespValue::BulkString(b"PSYNC".to_vec()),
            RespValue::BulkString(b"?".to_vec()),
            RespValue::BulkString(b"-1".to_vec())
        ]))
    ];

    let mut buf = [0u8; 512];
    for cmd in &commands[..commands.len() - 1] {
        stream.write_all(cmd.serialize().as_slice()).await.expect("Failed to send command");
        let n = stream.read(&mut buf).await.expect("Failed to read response");
        eprintln!("[repl] handshake response ({} bytes): {:?}", n, String::from_utf8_lossy(&buf[..n]));
    }

    stream.write_all(commands.last().unwrap().serialize().as_slice()).await.unwrap();
    let mut n = stream.read(&mut buf).await.expect("Failed to read FULLRESYNC");

    while buf[..n].iter().position(|&b| b == b'$').is_none() {
        let more = stream.read(&mut buf[n..]).await.expect("Failed to read more");
        n += more;
    }
    let rdb_start = buf[..n].iter().position(|&b| b == b'$').unwrap();
    let header_end = rdb_start + buf[rdb_start..n].windows(2)
        .position(|w| w == b"\r\n").unwrap() + 2;
    let rdb_len: usize = std::str::from_utf8(&buf[rdb_start + 1..header_end - 2])
        .unwrap().parse().unwrap();

    let mut accumulator: Vec<u8> = buf[header_end + rdb_len..n].to_vec();
    loop {
        while !accumulator.is_empty() {
            match parse_resp_bytes(&accumulator) {
                Ok((val, remainder)) => {
                    let remaining = remainder.to_vec();
                    let bytes_consumed = (accumulator.len() - remainder.len()) as u64;
                    let RespValue::Array(deque) = val else { panic!("Expected array"); };
                    let arr: Vec<RespValue> = deque.iter().cloned().collect();
                    let response = handle_cmd(&arr, storage.clone(), channels.clone(), repl_state.clone()).await;
                    accumulator = remaining;
                    repl_state.write().await.repl_offset += bytes_consumed;

                    match &arr[0] {
                        RespValue::BulkString(cmd) => {
                            if cmd == b"REPLCONF" {
                                stream.write_all(response.serialize().as_slice()).await.unwrap();
                            }
                        }
                        _ => panic!("Should not reach"),
                    }
                }
                Err(_) => break,
            }
        }

        let mut buf = [0u8; 1024];
        match stream.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => accumulator.extend_from_slice(&buf[..n]),
            Err(e) => { eprintln!("[repl] read error: {:?}", e); break; }
        }
    }
}
