#![allow(unused_imports)]

use redis_rs::{handle_connection, run_replication, Args};
use redis_rs::storage::Storage;
use redis_rs::task_communication::Channels;
use redis_rs::replication::{ReplicaInfo, ReplicationState, ReplicationStateHandle};

use std::collections::HashMap;
use std::io::Result;
use std::sync::Arc;
use tokio::net::{TcpListener};
use tokio::sync::RwLock;
use clap::Parser;
use redis_rs::resp_types::RespValue;

fn parse_repl_info(host_port: &String) -> ReplicaInfo {
    let mut parts = host_port.split_whitespace();
    let host = parts.next().unwrap().to_string();
    let port = parts.next().unwrap().parse().unwrap();

    ReplicaInfo {
        m_host: host,
        m_port: port
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let storage: Storage = Arc::new(RwLock::new(HashMap::new()));
    let channels: Channels = Arc::new(RwLock::new(HashMap::new()));
    let args = Args::parse();
    let repl_state: ReplicationStateHandle = match &args.replicaof {
        Some(s) => {
            let info = parse_repl_info(s);
            let r_state_tmp = Arc::new(RwLock::new(ReplicationState::new_replica(info.clone())));
            tokio::spawn(run_replication(info.clone(), args.port, storage.clone(), channels.clone(), r_state_tmp.clone()));
            r_state_tmp
        },
        None => Arc::new(RwLock::new(ReplicationState::new_master())),
    };

    let listener = TcpListener::bind(format!("{}:{}", args.host, args.port)).await?;
    loop {
        let socket = listener.accept().await?.0;

        let storage_clone = Arc::clone(&storage);
        let channels_clone = Arc::clone(&channels);
        let repl_state_clone = Arc::clone(&repl_state);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, storage_clone, channels_clone, repl_state_clone).await {
                eprintln!("connection error: {:?}", e);
            }
        });
    }
}
