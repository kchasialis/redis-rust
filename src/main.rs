use redis_rs::handle_connection;
use redis_rs::storage::Storage;
use redis_rs::task_communication::Channels;

use std::collections::HashMap;
use std::io::Result;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() -> Result<()> {
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