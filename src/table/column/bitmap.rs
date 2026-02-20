use bytemuck::cast_slice;

use crate::core::MurrError;

pub(super) struct NullBitmap<'a> {
    data: Option<&'a [u32]>,
}

impl NullBitmap<'_> {
    pub fn is_valid(&self, idx: u32) -> bool {
        match self.data {
            None => true,
            Some(words) => {
                let word_idx = (idx / 32) as usize;
                let bit_idx = idx % 32;
                (words[word_idx] >> bit_idx) & 1 == 1
            }
        }
    }
}

/// Parse the null bitmap from `data` at the given byte offset and word count.
/// The offset must be 4-byte aligned.
pub(super) fn parse_null_bitmap<'a>(
    data: &'a [u8],
    offset: usize,
    bitmap_size: u32,
    nullable: bool,
    type_name: &str,
) -> Result<NullBitmap<'a>, MurrError> {
    if !nullable || bitmap_size == 0 {
        return Ok(NullBitmap { data: None });
    }

    let byte_len = bitmap_size as usize * 4;
    if offset + byte_len > data.len() {
        return Err(MurrError::TableError(format!(
            "{type_name} segment truncated at null_bitmap"
        )));
    }

    let words: &[u32] = cast_slice(&data[offset..offset + byte_len]);
    Ok(NullBitmap { data: Some(words) })
}

/// Build null bitmap words from a null predicate.
/// Returns one bit per value: bit set = valid (not null).
/// Returns empty vec if no nulls exist (defers allocation until first null).
pub(super) fn build_bitmap_words(len: usize, is_null: impl Fn(usize) -> bool) -> Vec<u32> {
    let mut words: Option<Vec<u32>> = None;
    for i in 0..len {
        if is_null(i) {
            words.get_or_insert_with(|| {
                // First null: allocate and backfill all prior values as valid.
                let word_count = (len + 31) / 32;
                let mut v = vec![0u32; word_count];
                for j in 0..i {
                    v[j / 32] |= 1 << (j % 32);
                }
                v
            });
            // null → bit stays 0
        } else if let Some(ref mut w) = words {
            w[i / 32] |= 1 << (i % 32);
        }
    }
    words.unwrap_or_default()
}

/// Read a native-endian u32 from `data` at byte `offset`.
pub(crate) fn read_u32(data: &[u8], offset: usize) -> u32 {
    bytemuck::pod_read_unaligned::<u32>(&data[offset..offset + 4])
}

/// Number of padding bytes needed to align `len` up to 4 bytes.
pub(super) fn align4_padding(len: usize) -> usize {
    (4 - (len % 4)) % 4
}
