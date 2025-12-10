//! Manifest - Tracks database metadata and SSTable files

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};
use crate::error::Result;

#[derive(Serialize, Deserialize)]
pub struct Manifest {
    path: PathBuf,
    version: u64,
    sstables: HashMap<usize, Vec<PathBuf>>, // level -> files
    sequence: u64,
}

impl Manifest {
    pub fn open(data_dir: &Path) -> Result<Self> {
        let path = data_dir.join("MANIFEST");

        if path.exists() {
            let file = File::open(&path)?;
            let reader = BufReader::new(file);
            let manifest: Manifest = serde_json::from_reader(reader)?;
            Ok(manifest)
        } else {
            Ok(Self {
                path,
                version: 1,
                sstables: HashMap::new(),
                sequence: 0,
            })
        }
    }

    pub fn add_sstable(&mut self, level: usize, path: &Path) -> Result<()> {
        self.sstables
            .entry(level)
            .or_insert_with(Vec::new)
            .push(path.to_path_buf());
        self.save()
    }

    pub fn remove_sstable(&mut self, level: usize, path: &Path) -> Result<()> {
        if let Some(files) = self.sstables.get_mut(&level) {
            files.retain(|p| p != path);
        }
        self.save()
    }

    pub fn sstables(&self) -> impl Iterator<Item = (usize, &Vec<PathBuf>)> {
        self.sstables.iter().map(|(k, v)| (*k, v))
    }

    fn save(&self) -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, self)?;
        Ok(())
    }
}
