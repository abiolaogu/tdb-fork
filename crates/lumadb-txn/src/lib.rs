//! Distributed transaction support
//!
//! Provides:
//! - MVCC (Multi-Version Concurrency Control)
//! - 2PC (Two-Phase Commit)
//! - Lock management
//! - Timestamp Oracle

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use parking_lot::RwLock;
use tracing::{info, debug};

use lumadb_common::error::{Result, Error, TransactionError};
use lumadb_common::types::Timestamp;

/// Transaction coordinator
pub struct TransactionCoordinator {
    /// Timestamp oracle
    tso: TimestampOracle,
    /// Active transactions
    active: DashMap<u64, Transaction>,
    /// Lock manager
    locks: LockManager,
}

impl TransactionCoordinator {
    /// Create a new transaction coordinator
    pub fn new() -> Self {
        Self {
            tso: TimestampOracle::new(),
            active: DashMap::new(),
            locks: LockManager::new(),
        }
    }

    /// Begin a new transaction
    pub fn begin(&self) -> Transaction {
        let txn_id = self.tso.next();
        let start_ts = self.tso.get_timestamp();

        let txn = Transaction {
            id: txn_id,
            start_ts,
            commit_ts: None,
            state: RwLock::new(TxnState::Active),
            writes: RwLock::new(Vec::new()),
            reads: RwLock::new(Vec::new()),
        };

        self.active.insert(txn_id, txn.clone());
        txn
    }

    /// Commit a transaction
    pub async fn commit(&self, txn_id: u64) -> Result<()> {
        let txn = self
            .active
            .get(&txn_id)
            .ok_or(Error::Transaction(TransactionError::Aborted(
                "Transaction not found".to_string(),
            )))?;

        // Check for conflicts
        for write in txn.writes.read().iter() {
            if !self.locks.try_lock(txn_id, &write.key, LockType::Write)? {
                return Err(Error::Transaction(TransactionError::Conflict(
                    "Write conflict".to_string(),
                )));
            }
        }

        // Get commit timestamp
        let commit_ts = self.tso.get_timestamp();

        // Update transaction state
        *txn.state.write() = TxnState::Committed;

        // Release locks
        for write in txn.writes.read().iter() {
            self.locks.unlock(txn_id, &write.key);
        }

        self.active.remove(&txn_id);

        Ok(())
    }

    /// Rollback a transaction
    pub async fn rollback(&self, txn_id: u64) -> Result<()> {
        if let Some((_, txn)) = self.active.remove(&txn_id) {
            *txn.state.write() = TxnState::RolledBack;

            // Release all locks
            for write in txn.writes.read().iter() {
                self.locks.unlock(txn_id, &write.key);
            }
        }

        Ok(())
    }
}

impl Default for TransactionCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Transaction
#[derive(Debug)]
pub struct Transaction {
    pub id: u64,
    pub start_ts: Timestamp,
    pub commit_ts: Option<Timestamp>,
    state: RwLock<TxnState>,
    writes: RwLock<Vec<WriteIntent>>,
    reads: RwLock<Vec<ReadSet>>,
}

impl Clone for Transaction {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            start_ts: self.start_ts,
            commit_ts: self.commit_ts,
            state: RwLock::new(*self.state.read()),
            writes: RwLock::new(self.writes.read().clone()),
            reads: RwLock::new(self.reads.read().clone()),
        }
    }
}

impl Transaction {
    /// Add a write intent
    pub fn write(&self, key: Vec<u8>, value: Vec<u8>) {
        self.writes.write().push(WriteIntent { key, value });
    }

    /// Add to read set
    pub fn read(&self, key: Vec<u8>, version: Timestamp) {
        self.reads.write().push(ReadSet { key, version });
    }

    /// Check if transaction is active
    pub fn is_active(&self) -> bool {
        *self.state.read() == TxnState::Active
    }
}

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxnState {
    Active,
    Committed,
    RolledBack,
}

/// Write intent
#[derive(Debug, Clone)]
struct WriteIntent {
    key: Vec<u8>,
    value: Vec<u8>,
}

/// Read set entry
#[derive(Debug, Clone)]
struct ReadSet {
    key: Vec<u8>,
    version: Timestamp,
}

/// Timestamp Oracle for global ordering
pub struct TimestampOracle {
    counter: AtomicU64,
}

impl TimestampOracle {
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(chrono::Utc::now().timestamp_millis() as u64),
        }
    }

    /// Get next unique ID
    pub fn next(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }

    /// Get current timestamp
    pub fn get_timestamp(&self) -> Timestamp {
        self.counter.load(Ordering::SeqCst) as i64
    }
}

impl Default for TimestampOracle {
    fn default() -> Self {
        Self::new()
    }
}

/// Lock manager
struct LockManager {
    locks: DashMap<Vec<u8>, Lock>,
}

struct Lock {
    holder: u64,
    lock_type: LockType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockType {
    Read,
    Write,
}

impl LockManager {
    fn new() -> Self {
        Self {
            locks: DashMap::new(),
        }
    }

    fn try_lock(&self, txn_id: u64, key: &[u8], lock_type: LockType) -> Result<bool> {
        let key = key.to_vec();

        if let Some(existing) = self.locks.get(&key) {
            if existing.holder == txn_id {
                return Ok(true);
            }

            // Check for conflicts
            if lock_type == LockType::Write || existing.lock_type == LockType::Write {
                return Ok(false);
            }
        }

        self.locks.insert(key, Lock { holder: txn_id, lock_type });
        Ok(true)
    }

    fn unlock(&self, txn_id: u64, key: &[u8]) {
        let key = key.to_vec();
        if let Some(lock) = self.locks.get(&key) {
            if lock.holder == txn_id {
                drop(lock);
                self.locks.remove(&key);
            }
        }
    }
}
