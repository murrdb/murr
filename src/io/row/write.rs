use crate::io::schema::{SegmentColumnSchema, SegmentSchema};
use crate::io::store::KeyValue;

pub struct WriteRow<'a> {
    pub schema: &'a SegmentSchema,
    pub key: Vec<u8>,
    pub bytes: Vec<u8>,
}

impl<'a> From<WriteRow<'a>> for KeyValue {
    fn from(wr: WriteRow<'a>) -> Self {
        KeyValue {
            key: wr.key,
            value: wr.bytes,
        }
    }
}

impl<'a> WriteRow<'a> {
    pub fn new(schema: &'a SegmentSchema, key: &str) -> Self {
        let mut bytes = vec![0u8; schema.bitset_size + schema.capacity];
        bytes[..schema.bitset_size].fill(0xFF);
        Self {
            schema,
            key: key.as_bytes().to_vec(),
            bytes,
        }
    }

    pub fn set_non_null(&mut self, column: &SegmentColumnSchema) {
        let idx = column.index as usize;
        let byte = idx / 8;
        let bit = (idx % 8) as u8;
        self.bytes[byte] &= !(1 << bit);
    }

    pub fn write_static<T: bytemuck::NoUninit>(&mut self, column: &SegmentColumnSchema, value: T) {
        self.set_non_null(column);
        let start = self.schema.bitset_size + column.offset as usize;
        let end = start + std::mem::size_of::<T>();
        self.bytes[start..end].copy_from_slice(bytemuck::bytes_of(&value));
    }

    pub fn write_dynamic(&mut self, column: &SegmentColumnSchema, value: &[u8]) {
        self.set_non_null(column);
        let payload_rel = (self.bytes.len() - self.schema.bitset_size) as u32;
        let slot = self.schema.bitset_size + column.offset as usize;
        self.bytes[slot..slot + 4].copy_from_slice(&payload_rel.to_le_bytes());
        self.bytes
            .extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.bytes.extend_from_slice(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DTypeName;
    use crate::io::row::read::ReadRow;

    fn col(index: u32, dtype: DTypeName, name: &str, offset: u32) -> SegmentColumnSchema {
        SegmentColumnSchema {
            index,
            dtype,
            name: name.into(),
            offset,
        }
    }

    #[test]
    fn roundtrip_static_f32_f64() {
        let cols = vec![
            col(0, DTypeName::Float32, "x", 0),
            col(1, DTypeName::Float64, "y", 4),
        ];
        let schema = SegmentSchema::new(&cols);
        let mut w = WriteRow::new(&schema, "");
        w.write_static(&cols[0], 1.5f32);
        w.write_static(&cols[1], -3.25f64);

        let r = ReadRow::new(&schema, &w.bytes);
        assert_eq!(r.read_static::<f32>(&cols[0]), 1.5);
        assert_eq!(r.read_static::<f64>(&cols[1]), -3.25);
    }

    #[test]
    fn roundtrip_dynamic_utf8() {
        let cols = vec![
            col(0, DTypeName::Utf8, "a", 0),
            col(1, DTypeName::Utf8, "b", 4),
        ];
        let schema = SegmentSchema::new(&cols);
        let mut w = WriteRow::new(&schema, "");
        w.write_dynamic(&cols[0], b"");
        w.write_dynamic(&cols[1], "δ-unicode".as_bytes());

        let r = ReadRow::new(&schema, &w.bytes);
        assert_eq!(r.read_dynamic(&cols[0]), b"");
        assert_eq!(r.read_dynamic(&cols[1]), "δ-unicode".as_bytes());
    }

    #[test]
    fn roundtrip_mixed_f32_utf8() {
        let cols = vec![
            col(0, DTypeName::Float32, "x", 0),
            col(1, DTypeName::Utf8, "s", 4),
        ];
        let schema = SegmentSchema::new(&cols);
        let mut w = WriteRow::new(&schema, "");
        w.write_static(&cols[0], 42.5f32);
        w.write_dynamic(&cols[1], b"hello");

        let r = ReadRow::new(&schema, &w.bytes);
        assert!(!r.is_null(&cols[0]));
        assert!(!r.is_null(&cols[1]));
        assert_eq!(r.read_static::<f32>(&cols[0]), 42.5);
        assert_eq!(r.read_dynamic(&cols[1]), b"hello");
    }

    #[test]
    fn roundtrip_with_nulls() {
        let cols = vec![
            col(0, DTypeName::Float32, "x", 0),
            col(1, DTypeName::Utf8, "s", 4),
        ];
        let schema = SegmentSchema::new(&cols);

        let mut both = WriteRow::new(&schema, "");
        both.write_static(&cols[0], 1.0f32);
        both.write_dynamic(&cols[1], b"hi");
        let r = ReadRow::new(&schema, &both.bytes);
        assert!(!r.is_null(&cols[0]));
        assert!(!r.is_null(&cols[1]));

        let mut only_float = WriteRow::new(&schema, "");
        only_float.write_static(&cols[0], 7.5f32);
        let r = ReadRow::new(&schema, &only_float.bytes);
        assert!(!r.is_null(&cols[0]));
        assert!(r.is_null(&cols[1]));
        assert_eq!(r.read_static::<f32>(&cols[0]), 7.5);

        let none = WriteRow::new(&schema, "");
        let r = ReadRow::new(&schema, &none.bytes);
        assert!(r.is_null(&cols[0]));
        assert!(r.is_null(&cols[1]));
    }
}
