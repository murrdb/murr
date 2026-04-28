use lean_string::LeanString;

use crate::io3::table::index::RowLocation;

pub struct SegmentKey {
    pub key: LeanString,
    pub location: RowLocation,
}

pub struct SegmentKeyBytes {
    pub bytes: Vec<u8>,
}
impl SegmentKeyBytes {
    pub fn with_capacity(capacity: usize) -> Self {
        SegmentKeyBytes {
            bytes: Vec::with_capacity(capacity),
        }
    }
    pub fn read_keys(&self, segment_id: u32) -> impl Iterator<Item = SegmentKey> + '_ {
        SegmentKeyIter {
            bytes: &self.bytes,
            cursor: 0,
            segment_id,
        }
    }
    pub fn write_key(&mut self, key: &str, offset: u32, size: u32) {
        self.bytes
            .extend_from_slice(&(key.len() as u32).to_le_bytes());
        self.bytes.extend_from_slice(key.as_bytes());
        self.bytes.extend_from_slice(&offset.to_le_bytes());
        self.bytes.extend_from_slice(&size.to_le_bytes());
    }
    pub fn len(&self) -> usize {
        self.bytes.len()
    }
}

struct SegmentKeyIter<'a> {
    bytes: &'a [u8],
    cursor: usize,
    segment_id: u32,
}

impl<'a> Iterator for SegmentKeyIter<'a> {
    type Item = SegmentKey;

    fn next(&mut self) -> Option<SegmentKey> {
        if self.cursor >= self.bytes.len() {
            return None;
        }
        let key_len =
            u32::from_le_bytes(self.bytes[self.cursor..self.cursor + 4].try_into().ok()?) as usize;
        self.cursor += 4;
        let key_bytes = &self.bytes[self.cursor..self.cursor + key_len];
        self.cursor += key_len;
        let offset = u32::from_le_bytes(self.bytes[self.cursor..self.cursor + 4].try_into().ok()?);
        self.cursor += 4;
        let size = u32::from_le_bytes(self.bytes[self.cursor..self.cursor + 4].try_into().ok()?);
        self.cursor += 4;
        let key = LeanString::from(std::str::from_utf8(key_bytes).ok()?);
        Some(SegmentKey {
            key,
            location: RowLocation {
                segment_id: self.segment_id,
                offset,
                size,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_keys_roundtrip() {
        let mut buf = SegmentKeyBytes::with_capacity(64);
        buf.write_key("k0", 0, 12);
        buf.write_key("k1", 16, 20);
        buf.write_key("longer_key_42", 36, 8);

        let collected: Vec<SegmentKey> = buf.read_keys(7).collect();
        assert_eq!(collected.len(), 3);

        assert_eq!(collected[0].key.as_str(), "k0");
        assert_eq!(collected[0].location.segment_id, 7);
        assert_eq!(collected[0].location.offset, 0);
        assert_eq!(collected[0].location.size, 12);

        assert_eq!(collected[1].key.as_str(), "k1");
        assert_eq!(collected[1].location.offset, 16);
        assert_eq!(collected[1].location.size, 20);

        assert_eq!(collected[2].key.as_str(), "longer_key_42");
        assert_eq!(collected[2].location.offset, 36);
        assert_eq!(collected[2].location.size, 8);
    }

    #[test]
    fn read_keys_empty() {
        let buf = SegmentKeyBytes::with_capacity(0);
        assert_eq!(buf.read_keys(0).count(), 0);
    }
}
