//! Compaction strategies - Leveled and Universal

use crate::config::CompactionStyle;

pub struct CompactionManager {
    style: CompactionStyle,
    num_levels: usize,
}

impl CompactionManager {
    pub fn new(style: CompactionStyle, num_levels: usize) -> Self {
        Self { style, num_levels }
    }

    pub fn should_compact(&self, level: usize, file_count: usize, total_size: usize) -> bool {
        match self.style {
            CompactionStyle::Leveled => {
                if level == 0 {
                    file_count >= 4
                } else {
                    // Size ratio trigger
                    let max_size = 256 * 1024 * 1024 * 10_usize.pow(level as u32);
                    total_size > max_size
                }
            }
            CompactionStyle::Universal => {
                file_count >= 4
            }
            CompactionStyle::Fifo => {
                false // FIFO doesn't compact, just deletes old
            }
        }
    }

    pub fn pick_compaction(&self, level: usize) -> Option<CompactionTask> {
        Some(CompactionTask {
            source_level: level,
            target_level: (level + 1).min(self.num_levels - 1),
        })
    }
}

pub struct CompactionTask {
    pub source_level: usize,
    pub target_level: usize,
}
