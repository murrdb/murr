use arrow::array::Array;
use bytemuck::cast_slice;

use crate::core::MurrError;

pub(super) struct NullBitmap<'a> {
    data: &'a [u64],
}

impl<'a> NullBitmap<'a> {
    pub fn is_valid(&self, idx: u64) -> bool {
        let word_idx = (idx / 64) as usize;
        let bit_idx = idx % 64;
        (self.data[word_idx] >> bit_idx) & 1 == 1
    }

    /// Parse a null bitmap from `data` at the given byte offset and byte count.
    /// The offset must be 8-byte aligned.
    /// Returns `None` if the column is non-nullable or the segment has no bitmap.
    pub fn parse(
        data: &'a [u8],
        offset: usize,
        bitmap_size: u32,
        nullable: bool,
        type_name: &str,
    ) -> Result<Option<NullBitmap<'a>>, MurrError> {
        if !nullable || bitmap_size == 0 {
            return Ok(None);
        }

        let byte_len = bitmap_size as usize;
        if offset + byte_len > data.len() {
            return Err(MurrError::TableError(format!(
                "{type_name} segment truncated at null_bitmap"
            )));
        }

        let words: &[u64] = cast_slice(&data[offset..offset + byte_len]);
        Ok(Some(NullBitmap { data: words }))
    }

    /// Build a serialized null bitmap from an Arrow array.
    /// Returns one bit per value: bit set = valid (not null).
    /// Returns empty vec if no nulls exist (defers allocation until first null).
    pub fn write(values: &dyn Array) -> Vec<u8> {
        let len = values.len();
        let mut words: Option<Vec<u64>> = None;
        for i in 0..len {
            if values.is_null(i) {
                words.get_or_insert_with(|| {
                    // First null: allocate and backfill all prior values as valid.
                    let word_count = len.div_ceil(64);
                    let mut v = vec![0u64; word_count];
                    for j in 0..i {
                        v[j / 64] |= 1 << (j % 64);
                    }
                    v
                });
                // null → bit stays 0
            } else if let Some(ref mut w) = words {
                w[i / 64] |= 1 << (i % 64);
            }
        }
        match words {
            Some(w) => cast_slice(&w).to_vec(),
            None => Vec::new(),
        }
    }
}

/// Read a native-endian u32 from `data` at byte `offset`.
pub(crate) fn read_u32(data: &[u8], offset: usize) -> u32 {
    bytemuck::pod_read_unaligned::<u32>(&data[offset..offset + 4])
}

/// Number of padding bytes needed to align `len` up to 8 bytes.
pub(super) fn align8_padding(len: usize) -> usize {
    (8 - (len % 8)) % 8
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;

    #[test]
    fn test_write_no_nulls() {
        let array = Int32Array::from(vec![1, 2, 3]);
        let bytes = NullBitmap::write(&array);
        assert!(bytes.is_empty());
    }

    #[test]
    fn test_write_with_nulls() {
        let array = Int32Array::from(vec![Some(1), None, Some(3), Some(4)]);
        let bytes = NullBitmap::write(&array);
        assert_eq!(bytes.len(), 8); // one u64 word
        let words: &[u64] = cast_slice(&bytes);
        // bit 0 set (valid), bit 1 clear (null), bit 2 set, bit 3 set = 0b1101 = 13
        assert_eq!(words[0], 0b1101);
    }

    #[test]
    fn test_write_all_nulls() {
        let array = Int32Array::from(vec![None, None, None]);
        let bytes = NullBitmap::write(&array);
        assert_eq!(bytes.len(), 8);
        let words: &[u64] = cast_slice(&bytes);
        assert_eq!(words[0], 0);
    }

    #[test]
    fn test_parse_round_trip() {
        let array = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);
        let bytes = NullBitmap::write(&array);

        let bitmap = NullBitmap::parse(&bytes, 0, bytes.len() as u32, true, "test")
            .unwrap()
            .unwrap();
        assert!(bitmap.is_valid(0));
        assert!(!bitmap.is_valid(1));
        assert!(bitmap.is_valid(2));
        assert!(!bitmap.is_valid(3));
        assert!(bitmap.is_valid(4));
    }

    #[test]
    fn test_parse_non_nullable() {
        let bitmap = NullBitmap::parse(&[], 0, 0, false, "test").unwrap();
        assert!(bitmap.is_none());
    }

    #[test]
    fn test_parse_nullable_no_nulls() {
        let bitmap = NullBitmap::parse(&[], 0, 0, true, "test").unwrap();
        assert!(bitmap.is_none());
    }

    #[test]
    fn test_boundary_64_values() {
        let values: Vec<Option<i32>> = (0..64)
            .map(|i| if i == 63 { None } else { Some(i) })
            .collect();
        let array = Int32Array::from(values);
        let bytes = NullBitmap::write(&array);
        assert_eq!(bytes.len(), 8); // exactly one u64

        let bitmap = NullBitmap::parse(&bytes, 0, bytes.len() as u32, true, "test")
            .unwrap()
            .unwrap();
        for i in 0..63u64 {
            assert!(bitmap.is_valid(i), "expected valid at {i}");
        }
        assert!(!bitmap.is_valid(63));
    }

    #[test]
    fn test_boundary_65_values() {
        let values: Vec<Option<i32>> = (0..65)
            .map(|i| if i == 64 { None } else { Some(i) })
            .collect();
        let array = Int32Array::from(values);
        let bytes = NullBitmap::write(&array);
        assert_eq!(bytes.len(), 16); // two u64 words

        let bitmap = NullBitmap::parse(&bytes, 0, bytes.len() as u32, true, "test")
            .unwrap()
            .unwrap();
        for i in 0..64u64 {
            assert!(bitmap.is_valid(i), "expected valid at {i}");
        }
        assert!(!bitmap.is_valid(64));
    }
}
