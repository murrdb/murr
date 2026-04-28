use serde::{Deserialize, Serialize};

pub use crate::core::DType;
use crate::io::directory::ReadRequest;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OffsetSize {
    pub offset: u32,
    pub size: u32,
}

impl Into<ReadRequest> for OffsetSize {
    fn into(self) -> ReadRequest {
        ReadRequest {
            offset: self.offset,
            size: self.size,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SegmentColumnSchema {
    pub index: u32,
    pub dtype: DType,
    pub name: String,
    pub offset: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentSchema {
    pub columns: Vec<SegmentColumnSchema>,
}

impl SegmentSchema {
    pub fn new(columns: &Vec<SegmentColumnSchema>) -> Self {
        SegmentSchema {
            columns: columns.clone(),
        }
    }

    pub fn capacity(&self) -> usize {
        self.columns.iter().map(|c| c.dtype.size()).sum::<usize>()
    }

    pub fn bitset_size(&self) -> usize {
        (1 + self.columns.len().div_ceil(8)) as usize
    }
}
