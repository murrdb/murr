use serde::{Deserialize, Serialize};

use crate::core::{DType, TableSchema};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SegmentColumnSchema {
    pub index: u32,
    pub dtype: DType,
    pub name: String,
    pub offset: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentSchema {
    pub capacity: usize,
    pub bitset_size: usize,
    pub columns: Vec<SegmentColumnSchema>,
}

impl SegmentSchema {
    pub fn new(columns: &[SegmentColumnSchema]) -> Self {
        SegmentSchema {
            columns: columns.to_vec(),
            capacity: columns.iter().map(|c| c.dtype.size()).sum::<usize>(),
            bitset_size: columns.len().div_ceil(8),
        }
    }
}

impl From<&TableSchema> for SegmentSchema {
    fn from(schema: &TableSchema) -> Self {
        let mut offset: u32 = 0;
        let columns: Vec<SegmentColumnSchema> = schema
            .columns
            .iter()
            .filter(|(name, _)| *name != &schema.key)
            .enumerate()
            .map(|(i, (name, col))| {
                let column = SegmentColumnSchema {
                    index: i as u32,
                    dtype: col.dtype,
                    name: name.clone(),
                    offset,
                };
                offset += col.dtype.size() as u32;
                column
            })
            .collect();
        SegmentSchema::new(&columns)
    }
}
