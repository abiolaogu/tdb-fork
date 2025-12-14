//! Integration tests for Redis protocol

use std::net::TcpStream;
use std::io::{Write, Read, BufReader, BufRead};

fn send_command(stream: &mut TcpStream, args: &[&str]) -> String {
    // Build RESP command
    let mut cmd = format!("*{}\r\n", args.len());
    for arg in args {
        cmd.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }
    
    stream.write_all(cmd.as_bytes()).unwrap();
    stream.flush().unwrap();
    
    // Read response
    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).unwrap_or(0);
    String::from_utf8_lossy(&buf[..n]).to_string()
}

#[test]
#[ignore] // Run with: cargo test --test redis_tests -- --ignored
fn test_redis_string_commands() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").expect("Failed to connect to Redis");
    
    // PING
    let resp = send_command(&mut stream, &["PING"]);
    assert!(resp.contains("PONG"));
    
    // SET/GET
    let resp = send_command(&mut stream, &["SET", "test_key", "test_value"]);
    assert!(resp.contains("OK"));
    
    let resp = send_command(&mut stream, &["GET", "test_key"]);
    assert!(resp.contains("test_value"));
    
    // INCR
    send_command(&mut stream, &["SET", "counter", "10"]);
    let resp = send_command(&mut stream, &["INCR", "counter"]);
    assert!(resp.contains("11"));
    
    // DEL
    let resp = send_command(&mut stream, &["DEL", "test_key", "counter"]);
    assert!(resp.contains("2"));
    
    // EXISTS
    let resp = send_command(&mut stream, &["EXISTS", "test_key"]);
    assert!(resp.contains("0"));
}

#[test]
#[ignore]
fn test_redis_list_commands() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").expect("Failed to connect to Redis");
    
    // RPUSH
    let resp = send_command(&mut stream, &["RPUSH", "mylist", "a", "b", "c"]);
    assert!(resp.contains("3"));
    
    // LLEN
    let resp = send_command(&mut stream, &["LLEN", "mylist"]);
    assert!(resp.contains("3"));
    
    // LRANGE
    let resp = send_command(&mut stream, &["LRANGE", "mylist", "0", "-1"]);
    assert!(resp.contains("a") && resp.contains("b") && resp.contains("c"));
    
    // LPOP
    let resp = send_command(&mut stream, &["LPOP", "mylist"]);
    assert!(resp.contains("a"));
    
    // Cleanup
    send_command(&mut stream, &["DEL", "mylist"]);
}

#[test]
#[ignore]
fn test_redis_set_commands() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").expect("Failed to connect to Redis");
    
    // SADD
    let resp = send_command(&mut stream, &["SADD", "myset", "one", "two", "three"]);
    assert!(resp.contains("3"));
    
    // SCARD
    let resp = send_command(&mut stream, &["SCARD", "myset"]);
    assert!(resp.contains("3"));
    
    // SISMEMBER
    let resp = send_command(&mut stream, &["SISMEMBER", "myset", "one"]);
    assert!(resp.contains("1"));
    
    let resp = send_command(&mut stream, &["SISMEMBER", "myset", "four"]);
    assert!(resp.contains("0"));
    
    // Cleanup
    send_command(&mut stream, &["DEL", "myset"]);
}

#[test]
#[ignore]
fn test_redis_hash_commands() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").expect("Failed to connect to Redis");
    
    // HSET
    let resp = send_command(&mut stream, &["HSET", "myhash", "field1", "value1", "field2", "value2"]);
    assert!(resp.contains("2"));
    
    // HGET
    let resp = send_command(&mut stream, &["HGET", "myhash", "field1"]);
    assert!(resp.contains("value1"));
    
    // HDEL
    let resp = send_command(&mut stream, &["HDEL", "myhash", "field1"]);
    assert!(resp.contains("1"));
    
    // Cleanup
    send_command(&mut stream, &["DEL", "myhash"]);
}

#[test]
#[ignore]
fn test_redis_sorted_set_commands() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").expect("Failed to connect to Redis");
    
    // ZADD
    let resp = send_command(&mut stream, &["ZADD", "myzset", "1", "one", "2", "two", "3", "three"]);
    assert!(resp.contains("3"));
    
    // ZCARD
    let resp = send_command(&mut stream, &["ZCARD", "myzset"]);
    assert!(resp.contains("3"));
    
    // ZRANGE
    let resp = send_command(&mut stream, &["ZRANGE", "myzset", "0", "-1"]);
    assert!(resp.contains("one"));
    
    // Cleanup
    send_command(&mut stream, &["DEL", "myzset"]);
}
