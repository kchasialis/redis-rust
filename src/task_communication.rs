use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use crate::resp_types::{RespKey, RespValue};
use crate::storage::StorageValue;
use std::collections::VecDeque;
use tokio::sync::mpsc::Sender;

pub struct WaiterRegistry {
    pub senders: VecDeque<Sender<RespValue>>
}

impl WaiterRegistry {
    pub fn new() -> Self {
        Self { senders: VecDeque::new() }
    }

    pub fn add_waiter(&mut self, sender: Sender<RespValue>) {
        self.senders.push_back(sender);
    }

    pub fn pop_waiter(&mut self) -> Option<Sender<RespValue>> {
        self.senders.pop_front()
    }
}

pub(crate) type Channels = Arc<RwLock<HashMap<RespKey, WaiterRegistry>>>;