//! Thread-per-core reactor for maximum performance

use std::sync::Arc;
use std::thread::JoinHandle;

use crossbeam::channel::{bounded, Receiver, Sender};
use parking_lot::RwLock;
use tracing::{info, debug};

/// Thread-per-core reactor
pub struct Reactor {
    /// Number of cores
    num_cores: usize,
    /// Worker threads
    workers: RwLock<Vec<Worker>>,
    /// Running state
    running: Arc<RwLock<bool>>,
}

struct Worker {
    /// Core ID this worker is pinned to
    core_id: usize,
    /// Task sender
    task_tx: Sender<Task>,
    /// Thread handle
    handle: Option<JoinHandle<()>>,
}

/// A task to execute on a core
pub enum Task {
    /// Process produce request
    Produce {
        topic: String,
        partition: i32,
        data: Vec<u8>,
        response_tx: Sender<Result<i64, String>>,
    },
    /// Process fetch request
    Fetch {
        topic: String,
        partition: i32,
        offset: i64,
        max_bytes: usize,
        response_tx: Sender<Result<Vec<u8>, String>>,
    },
    /// Shutdown
    Shutdown,
}

impl Reactor {
    /// Create a new reactor with one thread per core
    pub fn new() -> Self {
        let num_cores = num_cpus::get();
        info!("Initializing reactor with {} cores", num_cores);

        Self {
            num_cores,
            workers: RwLock::new(Vec::new()),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Start the reactor
    pub fn start(&self) {
        *self.running.write() = true;

        let mut workers = self.workers.write();
        let core_ids = core_affinity::get_core_ids().unwrap_or_default();

        for (i, core_id) in core_ids.into_iter().enumerate().take(self.num_cores) {
            let (task_tx, task_rx) = bounded::<Task>(10000);
            let running = self.running.clone();

            let handle = std::thread::spawn(move || {
                // Pin thread to core
                core_affinity::set_for_current(core_id);
                debug!("Worker {} pinned to core {:?}", i, core_id);

                // Event loop
                while *running.read() {
                    match task_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                        Ok(Task::Shutdown) => break,
                        Ok(task) => Self::handle_task(task),
                        Err(_) => continue, // Timeout, check if still running
                    }
                }

                debug!("Worker {} shutting down", i);
            });

            workers.push(Worker {
                core_id: i,
                task_tx,
                handle: Some(handle),
            });
        }
    }

    /// Handle a task
    fn handle_task(task: Task) {
        match task {
            Task::Produce { response_tx, .. } => {
                // Process produce request
                let _ = response_tx.send(Ok(0));
            }
            Task::Fetch { response_tx, .. } => {
                // Process fetch request
                let _ = response_tx.send(Ok(Vec::new()));
            }
            Task::Shutdown => {}
        }
    }

    /// Submit a task to a specific core
    pub fn submit(&self, core: usize, task: Task) -> Result<(), String> {
        let workers = self.workers.read();

        if core >= workers.len() {
            return Err(format!("Invalid core: {}", core));
        }

        workers[core]
            .task_tx
            .send(task)
            .map_err(|e| e.to_string())
    }

    /// Get the number of cores
    pub fn num_cores(&self) -> usize {
        self.num_cores
    }

    /// Choose a core for a partition (based on partition ID)
    pub fn core_for_partition(&self, partition: i32) -> usize {
        (partition as usize) % self.num_cores
    }

    /// Stop the reactor
    pub fn stop(&self) {
        *self.running.write() = false;

        let mut workers = self.workers.write();

        // Send shutdown to all workers
        for worker in workers.iter() {
            let _ = worker.task_tx.send(Task::Shutdown);
        }

        // Wait for workers to finish
        for worker in workers.iter_mut() {
            if let Some(handle) = worker.handle.take() {
                let _ = handle.join();
            }
        }

        workers.clear();
        info!("Reactor stopped");
    }
}

impl Default for Reactor {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        self.stop();
    }
}
