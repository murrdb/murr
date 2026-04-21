use serde::{Deserialize, Serialize};

pub use crate::core::DType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OffsetSize {
    pub offset: u32,
    pub size: u32,
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
    pub capacity: u32,
    pub bitset_size: u8,
    pub columns: Vec<SegmentColumnSchema>,
}

impl SegmentSchema {
    pub fn new(columns: &Vec<SegmentColumnSchema>) -> Self {
        let capacity = columns.iter().map(|c| c.dtype.size()).sum::<usize>();
        SegmentSchema {
            capacity: capacity as u32,
            bitset_size: (1 + columns.len().div_ceil(8)) as u8,
            columns: columns.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentFooter {
    pub version: u32,
    pub name: u32,
    pub schema: SegmentSchema,
    pub keys: OffsetSize,
    pub rows: OffsetSize,
}
