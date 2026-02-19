use crate::core::MurrError;

pub(super) struct NullBitmap<'a> {
    data: Option<&'a [u8]>,
}

impl NullBitmap<'_> {
    pub fn is_valid(&self, idx: u32) -> bool {
        match self.data {
            None => true,
            Some(bitmap) => {
                let word_idx = (idx / 32) as usize;
                let bit_idx = idx % 32;
                let word = read_u32_le(bitmap, word_idx * 4);
                (word >> bit_idx) & 1 == 1
            }
        }
    }
}

/// Parse the null bitmap trailer from segment data at `pos`.
///
/// Wire format at `pos`:
/// ```text
/// [null_bitmap_size: u32]            // count of u32 words (0 if non-nullable)
/// [null_bitmap: [u32; null_bitmap_size]]
/// ```
pub(super) fn parse_null_bitmap<'a>(
    data: &'a [u8],
    pos: usize,
    nullable: bool,
    type_name: &str,
) -> Result<(NullBitmap<'a>, usize), MurrError> {
    if pos + 4 > data.len() {
        return Err(MurrError::TableError(format!(
            "{type_name} segment truncated at null_bitmap_size"
        )));
    }
    let null_bitmap_size = read_u32_le(data, pos);
    let mut end = pos + 4;

    let nulls = if nullable && null_bitmap_size > 0 {
        let bitmap_byte_len = null_bitmap_size as usize * 4;
        if end + bitmap_byte_len > data.len() {
            return Err(MurrError::TableError(format!(
                "{type_name} segment truncated at null_bitmap"
            )));
        }
        let bitmap = &data[end..end + bitmap_byte_len];
        end += bitmap_byte_len;
        NullBitmap { data: Some(bitmap) }
    } else {
        NullBitmap { data: None }
    };

    Ok((nulls, end))
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

/// Write `[null_bitmap_size: u32][bitmap_words...]` to buf.
pub(super) fn write_bitmap(buf: &mut Vec<u8>, bitmap_words: &[u32]) {
    let null_bitmap_size = bitmap_words.len() as u32;
    buf.extend_from_slice(&null_bitmap_size.to_le_bytes());
    for word in bitmap_words {
        buf.extend_from_slice(&word.to_le_bytes());
    }
}

pub(super) fn read_u32_le(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
}

pub(super) fn read_i32_le(data: &[u8], offset: usize) -> i32 {
    i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
}

pub(super) fn read_f32_le(data: &[u8], offset: usize) -> f32 {
    f32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
}
