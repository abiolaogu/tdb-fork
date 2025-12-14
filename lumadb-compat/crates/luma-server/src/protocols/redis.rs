//! Redis RESP Protocol Implementation
//! Provides DragonflyDB-compatible wire protocol support

use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter, AsyncBufReadExt};
use std::sync::Arc;
use std::collections::HashMap;
use dashmap::DashMap;
use tracing::{info, error, debug, warn};

/// Redis data types
#[derive(Clone, Debug)]
pub enum RedisValue {
    String(String),
    Integer(i64),
    List(Vec<String>),
    Set(std::collections::HashSet<String>),
    Hash(HashMap<String, String>),
    SortedSet(Vec<(f64, String)>),
    Null,
}

/// Redis in-memory store
pub struct RedisStore {
    data: Arc<DashMap<String, RedisValue>>,
    expiry: Arc<DashMap<String, std::time::Instant>>,
}

impl RedisStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(DashMap::new()),
            expiry: Arc::new(DashMap::new()),
        }
    }

    pub fn get(&self, key: &str) -> Option<RedisValue> {
        // Check expiry
        if let Some(exp) = self.expiry.get(key) {
            if std::time::Instant::now() > *exp {
                self.data.remove(key);
                self.expiry.remove(key);
                return None;
            }
        }
        self.data.get(key).map(|v| v.value().clone())
    }

    pub fn set(&self, key: String, value: RedisValue) {
        self.data.insert(key, value);
    }

    pub fn set_ex(&self, key: String, value: RedisValue, seconds: u64) {
        self.data.insert(key.clone(), value);
        self.expiry.insert(key, std::time::Instant::now() + std::time::Duration::from_secs(seconds));
    }

    pub fn del(&self, key: &str) -> bool {
        self.expiry.remove(key);
        self.data.remove(key).is_some()
    }

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        // Simple pattern matching (only supports *)
        if pattern == "*" {
            self.data.iter().map(|r| r.key().clone()).collect()
        } else {
            let prefix = pattern.trim_end_matches('*');
            self.data.iter()
                .filter(|r| r.key().starts_with(prefix))
                .map(|r| r.key().clone())
                .collect()
        }
    }
}

/// RESP protocol values
#[derive(Debug, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<String>),
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            RespValue::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            RespValue::Error(e) => format!("-{}\r\n", e).into_bytes(),
            RespValue::Integer(i) => format!(":{}\r\n", i).into_bytes(),
            RespValue::BulkString(None) => b"$-1\r\n".to_vec(),
            RespValue::BulkString(Some(s)) => format!("${}\r\n{}\r\n", s.len(), s).into_bytes(),
            RespValue::Array(None) => b"*-1\r\n".to_vec(),
            RespValue::Array(Some(arr)) => {
                let mut result = format!("*{}\r\n", arr.len()).into_bytes();
                for item in arr {
                    result.extend(item.serialize());
                }
                result
            }
        }
    }
}

/// Parse RESP command from stream
async fn parse_command(reader: &mut BufReader<tokio::net::tcp::ReadHalf<'_>>) -> Result<Vec<String>, String> {
    let mut line = String::new();
    if reader.read_line(&mut line).await.map_err(|e| e.to_string())? == 0 {
        return Err("Connection closed".to_string());
    }

    let line = line.trim();
    if !line.starts_with('*') {
        // Inline command
        return Ok(line.split_whitespace().map(|s| s.to_string()).collect());
    }

    let count: usize = line[1..].parse().map_err(|_| "Invalid array length")?;
    let mut args = Vec::with_capacity(count);

    for _ in 0..count {
        let mut bulk_line = String::new();
        reader.read_line(&mut bulk_line).await.map_err(|e| e.to_string())?;
        let bulk_line = bulk_line.trim();
        
        if !bulk_line.starts_with('$') {
            return Err("Expected bulk string".to_string());
        }
        
        let len: usize = bulk_line[1..].parse().map_err(|_| "Invalid bulk length")?;
        let mut data = vec![0u8; len + 2]; // +2 for \r\n
        reader.read_exact(&mut data).await.map_err(|e| e.to_string())?;
        
        let s = String::from_utf8_lossy(&data[..len]).to_string();
        args.push(s);
    }

    Ok(args)
}

/// Execute Redis command
fn execute_command(store: &RedisStore, args: Vec<String>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR empty command".to_string());
    }

    let cmd = args[0].to_uppercase();
    match cmd.as_str() {
        "PING" => {
            if args.len() > 1 {
                RespValue::BulkString(Some(args[1].clone()))
            } else {
                RespValue::SimpleString("PONG".to_string())
            }
        }
        
        "GET" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            match store.get(&args[1]) {
                Some(RedisValue::String(s)) => RespValue::BulkString(Some(s)),
                Some(RedisValue::Integer(i)) => RespValue::BulkString(Some(i.to_string())),
                Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                None => RespValue::BulkString(None),
            }
        }

        "SET" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = args[1].clone();
            let value = args[2].clone();
            
            // Handle EX/PX options
            if args.len() > 4 && args[3].to_uppercase() == "EX" {
                if let Ok(seconds) = args[4].parse::<u64>() {
                    store.set_ex(key, RedisValue::String(value), seconds);
                    return RespValue::SimpleString("OK".to_string());
                }
            }
            
            store.set(key, RedisValue::String(value));
            RespValue::SimpleString("OK".to_string())
        }

        "DEL" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let count: i64 = args[1..].iter()
                .filter(|k| store.del(k))
                .count() as i64;
            RespValue::Integer(count)
        }

        "MGET" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let values: Vec<RespValue> = args[1..].iter()
                .map(|k| {
                    match store.get(k) {
                        Some(RedisValue::String(s)) => RespValue::BulkString(Some(s)),
                        _ => RespValue::BulkString(None),
                    }
                })
                .collect();
            RespValue::Array(Some(values))
        }

        "MSET" => {
            if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            for chunk in args[1..].chunks(2) {
                store.set(chunk[0].clone(), RedisValue::String(chunk[1].clone()));
            }
            RespValue::SimpleString("OK".to_string())
        }

        "INCR" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = &args[1];
            let current = match store.get(key) {
                Some(RedisValue::String(s)) => s.parse::<i64>().unwrap_or(0),
                Some(RedisValue::Integer(i)) => i,
                None => 0,
                _ => return RespValue::Error("WRONGTYPE".to_string()),
            };
            let new_val = current + 1;
            store.set(key.clone(), RedisValue::Integer(new_val));
            RespValue::Integer(new_val)
        }

        "KEYS" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let keys = store.keys(&args[1]);
            let values: Vec<RespValue> = keys.into_iter()
                .map(|k| RespValue::BulkString(Some(k)))
                .collect();
            RespValue::Array(Some(values))
        }

        "EXISTS" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let count: i64 = args[1..].iter()
                .filter(|k| store.get(k).is_some())
                .count() as i64;
            RespValue::Integer(count)
        }

        "INFO" => {
            let info = "# Server\r\nredis_version:7.0.0-lumadb\r\n# Clients\r\nconnected_clients:1\r\n";
            RespValue::BulkString(Some(info.to_string()))
        }

        "DECR" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = &args[1];
            let current = match store.get(key) {
                Some(RedisValue::String(s)) => s.parse::<i64>().unwrap_or(0),
                Some(RedisValue::Integer(i)) => i,
                None => 0,
                _ => return RespValue::Error("WRONGTYPE".to_string()),
            };
            let new_val = current - 1;
            store.set(key.clone(), RedisValue::Integer(new_val));
            RespValue::Integer(new_val)
        }

        "INCRBY" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = &args[1];
            let incr: i64 = args[2].parse().unwrap_or(0);
            let current = match store.get(key) {
                Some(RedisValue::String(s)) => s.parse::<i64>().unwrap_or(0),
                Some(RedisValue::Integer(i)) => i,
                None => 0,
                _ => return RespValue::Error("WRONGTYPE".to_string()),
            };
            let new_val = current + incr;
            store.set(key.clone(), RedisValue::Integer(new_val));
            RespValue::Integer(new_val)
        }

        "DECRBY" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = &args[1];
            let decr: i64 = args[2].parse().unwrap_or(0);
            let current = match store.get(key) {
                Some(RedisValue::String(s)) => s.parse::<i64>().unwrap_or(0),
                Some(RedisValue::Integer(i)) => i,
                None => 0,
                _ => return RespValue::Error("WRONGTYPE".to_string()),
            };
            let new_val = current - decr;
            store.set(key.clone(), RedisValue::Integer(new_val));
            RespValue::Integer(new_val)
        }

        "SETNX" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            if store.get(&args[1]).is_none() {
                store.set(args[1].clone(), RedisValue::String(args[2].clone()));
                RespValue::Integer(1)
            } else {
                RespValue::Integer(0)
            }
        }

        "SETEX" => {
            if args.len() < 4 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            if let Ok(seconds) = args[2].parse::<u64>() {
                store.set_ex(args[1].clone(), RedisValue::String(args[3].clone()), seconds);
                RespValue::SimpleString("OK".to_string())
            } else {
                RespValue::Error("ERR invalid expire time".to_string())
            }
        }

        "APPEND" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = args[1].clone();
            let append_val = &args[2];
            let new_val = match store.get(&key) {
                Some(RedisValue::String(s)) => format!("{}{}", s, append_val),
                None => append_val.clone(),
                _ => return RespValue::Error("WRONGTYPE".to_string()),
            };
            let len = new_val.len() as i64;
            store.set(key, RedisValue::String(new_val));
            RespValue::Integer(len)
        }

        "STRLEN" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            match store.get(&args[1]) {
                Some(RedisValue::String(s)) => RespValue::Integer(s.len() as i64),
                None => RespValue::Integer(0),
                _ => RespValue::Error("WRONGTYPE".to_string()),
            }
        }

        "GETSET" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = args[1].clone();
            let old = match store.get(&key) {
                Some(RedisValue::String(s)) => RespValue::BulkString(Some(s)),
                Some(RedisValue::Integer(i)) => RespValue::BulkString(Some(i.to_string())),
                None => RespValue::BulkString(None),
                _ => return RespValue::Error("WRONGTYPE".to_string()),
            };
            store.set(key, RedisValue::String(args[2].clone()));
            old
        }

        "RENAME" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            match store.get(&args[1]) {
                Some(val) => {
                    store.del(&args[1]);
                    store.set(args[2].clone(), val);
                    RespValue::SimpleString("OK".to_string())
                }
                None => RespValue::Error("ERR no such key".to_string()),
            }
        }

        "SCAN" => {
            // Simplified SCAN: returns all keys with cursor 0
            let _cursor: u64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
            let pattern = args.iter().position(|a| a.to_uppercase() == "MATCH")
                .map(|i| args.get(i + 1).cloned().unwrap_or_default())
                .unwrap_or_else(|| "*".to_string());
            let count = args.iter().position(|a| a.to_uppercase() == "COUNT")
                .and_then(|i| args.get(i + 1).and_then(|s| s.parse().ok()))
                .unwrap_or(10usize);
            
            let keys: Vec<RespValue> = store.keys(&pattern)
                .into_iter()
                .take(count)
                .map(|k| RespValue::BulkString(Some(k)))
                .collect();
            
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some("0".to_string())),
                RespValue::Array(Some(keys)),
            ]))
        }

        "ECHO" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            RespValue::BulkString(Some(args[1].clone()))
        }

        "CLIENT" => {
            // CLIENT subcommands
            if args.len() > 1 && args[1].to_uppercase() == "SETNAME" {
                RespValue::SimpleString("OK".to_string())
            } else if args.len() > 1 && args[1].to_uppercase() == "GETNAME" {
                RespValue::BulkString(None)
            } else {
                RespValue::SimpleString("OK".to_string())
            }
        }

        "SELECT" => {
            // Database selection (all map to same db in LumaDB)
            RespValue::SimpleString("OK".to_string())
        }

        "COMMAND" => {
            // Return empty array for compatibility
            RespValue::Array(Some(vec![]))
        }

        "QUIT" => {
            RespValue::SimpleString("OK".to_string())
        }

        // === List Commands ===
        "LPUSH" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = args[1].clone();
            let mut list = match store.get(&key) {
                Some(RedisValue::List(l)) => l,
                None => vec![],
                _ => return RespValue::Error("WRONGTYPE".to_string()),
            };
            for val in args[2..].iter().rev() {
                list.insert(0, val.clone());
            }
            let len = list.len() as i64;
            store.set(key, RedisValue::List(list));
            RespValue::Integer(len)
        }

        "RPUSH" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = args[1].clone();
            let mut list = match store.get(&key) {
                Some(RedisValue::List(l)) => l,
                None => vec![],
                _ => return RespValue::Error("WRONGTYPE".to_string()),
            };
            for val in &args[2..] {
                list.push(val.clone());
            }
            let len = list.len() as i64;
            store.set(key, RedisValue::List(list));
            RespValue::Integer(len)
        }

        "LPOP" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = args[1].clone();
            match store.get(&key) {
                Some(RedisValue::List(mut l)) => {
                    if l.is_empty() {
                        RespValue::BulkString(None)
                    } else {
                        let val = l.remove(0);
                        store.set(key, RedisValue::List(l));
                        RespValue::BulkString(Some(val))
                    }
                }
                None => RespValue::BulkString(None),
                _ => RespValue::Error("WRONGTYPE".to_string()),
            }
        }

        "RPOP" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = args[1].clone();
            match store.get(&key) {
                Some(RedisValue::List(mut l)) => {
                    if l.is_empty() {
                        RespValue::BulkString(None)
                    } else {
                        let val = l.pop().unwrap();
                        store.set(key, RedisValue::List(l));
                        RespValue::BulkString(Some(val))
                    }
                }
                None => RespValue::BulkString(None),
                _ => RespValue::Error("WRONGTYPE".to_string()),
            }
        }

        "LRANGE" => {
            if args.len() < 4 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = &args[1];
            let start: i64 = args[2].parse().unwrap_or(0);
            let stop: i64 = args[3].parse().unwrap_or(-1);
            
            match store.get(key) {
                Some(RedisValue::List(l)) => {
                    let len = l.len() as i64;
                    let start = if start < 0 { (len + start).max(0) as usize } else { start as usize };
                    let stop = if stop < 0 { (len + stop + 1).max(0) as usize } else { (stop + 1).min(len) as usize };
                    
                    let slice: Vec<RespValue> = l[start..stop.min(l.len())]
                        .iter()
                        .map(|s| RespValue::BulkString(Some(s.clone())))
                        .collect();
                    RespValue::Array(Some(slice))
                }
                None => RespValue::Array(Some(vec![])),
                _ => RespValue::Error("WRONGTYPE".to_string()),
            }
        }

        "LLEN" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            match store.get(&args[1]) {
                Some(RedisValue::List(l)) => RespValue::Integer(l.len() as i64),
                None => RespValue::Integer(0),
                _ => RespValue::Error("WRONGTYPE".to_string()),
            }
        }

        // === Set Commands ===
        "SADD" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = args[1].clone();
            let mut set = match store.get(&key) {
                Some(RedisValue::Set(s)) => s,
                None => std::collections::HashSet::new(),
                _ => return RespValue::Error("WRONGTYPE".to_string()),
            };
            let mut added = 0i64;
            for val in &args[2..] {
                if set.insert(val.clone()) {
                    added += 1;
                }
            }
            store.set(key, RedisValue::Set(set));
            RespValue::Integer(added)
        }

        "SMEMBERS" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            match store.get(&args[1]) {
                Some(RedisValue::Set(s)) => {
                    let members: Vec<RespValue> = s.into_iter()
                        .map(|m| RespValue::BulkString(Some(m)))
                        .collect();
                    RespValue::Array(Some(members))
                }
                None => RespValue::Array(Some(vec![])),
                _ => RespValue::Error("WRONGTYPE".to_string()),
            }
        }

        "SISMEMBER" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            match store.get(&args[1]) {
                Some(RedisValue::Set(s)) => {
                    RespValue::Integer(if s.contains(&args[2]) { 1 } else { 0 })
                }
                None => RespValue::Integer(0),
                _ => RespValue::Error("WRONGTYPE".to_string()),
            }
        }

        "SCARD" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            match store.get(&args[1]) {
                Some(RedisValue::Set(s)) => RespValue::Integer(s.len() as i64),
                None => RespValue::Integer(0),
                _ => RespValue::Error("WRONGTYPE".to_string()),
            }
        }

        // === Hash Commands ===
        "HSET" => {
            if args.len() < 4 || (args.len() - 2) % 2 != 0 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = args[1].clone();
            let mut hash = match store.get(&key) {
                Some(RedisValue::Hash(h)) => h,
                None => HashMap::new(),
                _ => return RespValue::Error("WRONGTYPE".to_string()),
            };
            let mut added = 0i64;
            for chunk in args[2..].chunks(2) {
                if hash.insert(chunk[0].clone(), chunk[1].clone()).is_none() {
                    added += 1;
                }
            }
            store.set(key, RedisValue::Hash(hash));
            RespValue::Integer(added)
        }

        "HGET" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            match store.get(&args[1]) {
                Some(RedisValue::Hash(h)) => {
                    match h.get(&args[2]) {
                        Some(v) => RespValue::BulkString(Some(v.clone())),
                        None => RespValue::BulkString(None),
                    }
                }
                None => RespValue::BulkString(None),
                _ => RespValue::Error("WRONGTYPE".to_string()),
            }
        }

        "HGETALL" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            match store.get(&args[1]) {
                Some(RedisValue::Hash(h)) => {
                    let pairs: Vec<RespValue> = h.into_iter()
                        .flat_map(|(k, v)| vec![
                            RespValue::BulkString(Some(k)),
                            RespValue::BulkString(Some(v)),
                        ])
                        .collect();
                    RespValue::Array(Some(pairs))
                }
                None => RespValue::Array(Some(vec![])),
                _ => RespValue::Error("WRONGTYPE".to_string()),
            }
        }

        "HDEL" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = args[1].clone();
            match store.get(&key) {
                Some(RedisValue::Hash(mut h)) => {
                    let mut removed = 0i64;
                    for field in &args[2..] {
                        if h.remove(field).is_some() {
                            removed += 1;
                        }
                    }
                    store.set(key, RedisValue::Hash(h));
                    RespValue::Integer(removed)
                }
                None => RespValue::Integer(0),
                _ => RespValue::Error("WRONGTYPE".to_string()),
            }
        }

        // === Sorted Set Commands ===
        "ZADD" => {
            if args.len() < 4 || (args.len() - 2) % 2 != 0 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = args[1].clone();
            let mut zset = match store.get(&key) {
                Some(RedisValue::SortedSet(z)) => z,
                None => vec![],
                _ => return RespValue::Error("WRONGTYPE".to_string()),
            };
            let mut added = 0i64;
            for chunk in args[2..].chunks(2) {
                let score: f64 = chunk[0].parse().unwrap_or(0.0);
                let member = chunk[1].clone();
                // Remove existing
                zset.retain(|(_, m)| m != &member);
                zset.push((score, member));
                added += 1;
            }
            zset.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            store.set(key, RedisValue::SortedSet(zset));
            RespValue::Integer(added)
        }

        "ZRANGE" => {
            if args.len() < 4 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = &args[1];
            let start: i64 = args[2].parse().unwrap_or(0);
            let stop: i64 = args[3].parse().unwrap_or(-1);
            let with_scores = args.len() > 4 && args[4].to_uppercase() == "WITHSCORES";
            
            match store.get(key) {
                Some(RedisValue::SortedSet(z)) => {
                    let len = z.len() as i64;
                    let start = if start < 0 { (len + start).max(0) as usize } else { start as usize };
                    let stop = if stop < 0 { (len + stop + 1).max(0) as usize } else { (stop + 1).min(len) as usize };
                    
                    let slice: Vec<RespValue> = z[start..stop.min(z.len())]
                        .iter()
                        .flat_map(|(score, member)| {
                            if with_scores {
                                vec![
                                    RespValue::BulkString(Some(member.clone())),
                                    RespValue::BulkString(Some(score.to_string())),
                                ]
                            } else {
                                vec![RespValue::BulkString(Some(member.clone()))]
                            }
                        })
                        .collect();
                    RespValue::Array(Some(slice))
                }
                None => RespValue::Array(Some(vec![])),
                _ => RespValue::Error("WRONGTYPE".to_string()),
            }
        }

        "ZCARD" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            match store.get(&args[1]) {
                Some(RedisValue::SortedSet(z)) => RespValue::Integer(z.len() as i64),
                None => RespValue::Integer(0),
                _ => RespValue::Error("WRONGTYPE".to_string()),
            }
        }

        // === Utility Commands ===
        "TYPE" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            match store.get(&args[1]) {
                Some(RedisValue::String(_)) | Some(RedisValue::Integer(_)) => RespValue::SimpleString("string".to_string()),
                Some(RedisValue::List(_)) => RespValue::SimpleString("list".to_string()),
                Some(RedisValue::Set(_)) => RespValue::SimpleString("set".to_string()),
                Some(RedisValue::Hash(_)) => RespValue::SimpleString("hash".to_string()),
                Some(RedisValue::SortedSet(_)) => RespValue::SimpleString("zset".to_string()),
                Some(RedisValue::Null) | None => RespValue::SimpleString("none".to_string()),
            }
        }

        "EXPIRE" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            if store.get(&args[1]).is_some() {
                if let Ok(seconds) = args[2].parse::<u64>() {
                    store.expiry.insert(args[1].clone(), std::time::Instant::now() + std::time::Duration::from_secs(seconds));
                    return RespValue::Integer(1);
                }
            }
            RespValue::Integer(0)
        }

        "TTL" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            match store.expiry.get(&args[1]) {
                Some(exp) => {
                    let remaining = exp.saturating_duration_since(std::time::Instant::now());
                    RespValue::Integer(remaining.as_secs() as i64)
                }
                None => {
                    if store.get(&args[1]).is_some() {
                        RespValue::Integer(-1) // No expiry
                    } else {
                        RespValue::Integer(-2) // Key doesn't exist
                    }
                }
            }
        }

        "DBSIZE" => {
            RespValue::Integer(store.data.len() as i64)
        }

        "FLUSHDB" => {
            store.data.clear();
            store.expiry.clear();
            RespValue::SimpleString("OK".to_string())
        }

        _ => RespValue::Error(format!("ERR unknown command '{}'", cmd)),
    }
}

/// Handle Redis connection
async fn handle_connection(mut socket: TcpStream, store: Arc<RedisStore>) {
    let (read_half, mut write_half) = socket.split();
    let mut reader = BufReader::new(read_half);

    loop {
        match parse_command(&mut reader).await {
            Ok(args) => {
                if args.is_empty() {
                    continue;
                }
                
                debug!("Redis command: {:?}", args);
                
                if args[0].to_uppercase() == "QUIT" {
                    let _ = write_half.write_all(&RespValue::SimpleString("OK".to_string()).serialize()).await;
                    break;
                }

                let response = execute_command(&store, args);
                if let Err(e) = write_half.write_all(&response.serialize()).await {
                    error!("Write error: {}", e);
                    break;
                }
            }
            Err(e) => {
                if e != "Connection closed" {
                    warn!("Parse error: {}", e);
                }
                break;
            }
        }
    }
}

/// Run Redis protocol server
pub async fn run(port: u16) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Redis Protocol Server listening on {}", addr);

    let store = Arc::new(RedisStore::new());

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        let store = store.clone();
        debug!("New Redis connection from {}", peer_addr);
        
        tokio::spawn(async move {
            handle_connection(socket, store).await;
        });
    }
}
