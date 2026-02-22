use std::alloc::System;
use std::collections::{HashMap, VecDeque};
use std::ops::{Add, Sub};
use std::sync::Arc;
use std::thread::current;
use tokio::sync::RwLock;
use std::time::{Duration, SystemTime};
use crate::resp_types::{RespKey, RespValue};

pub struct StorageValue {
    data: RespValue,
    insert_time: SystemTime,
    expiry_duration: Option<Duration>
}

impl StorageValue {
    pub fn new(data: RespValue, expiry_duration: Option<Duration>) -> StorageValue {
        StorageValue {
            data,
            insert_time: SystemTime::now(),
            expiry_duration
        }
    }

    pub fn data(&self) -> Option<&RespValue> {
        if let Some(expiry_duration_val) = self.expiry_duration {
            let current_time = SystemTime::now();
            let valid_until = self.insert_time.add(expiry_duration_val);
            if valid_until.gt(&current_time) {
                Some(&self.data)
            } else {
                None
            }
        } else {
            Some(&self.data)
        }
    }

    pub fn data_mut(&mut self) -> Option<&mut RespValue> {
        if let Some(expiry_duration_val) = self.expiry_duration {
            let current_time = SystemTime::now();
            let valid_until = self.insert_time.add(expiry_duration_val);
            if valid_until.gt(&current_time) {
                Some(&mut self.data)
            } else {
                None
            }
        } else {
            Some(&mut self.data)
        }
    }
}

pub(crate) type Storage = Arc<RwLock<HashMap<RespKey, StorageValue>>>;

