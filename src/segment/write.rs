use std::io::Write;

use crate::core::MurrError;

use super::format::{HEADER_SIZE, MAGIC, VERSION};

/// Builder for a `.seg` file. Collects named column payloads and serializes
/// them into the segment binary format.
#[derive(Default)]
pub struct WriteSegment {
    columns: Vec<(String, Vec<u8>)>,
}

impl WriteSegment {
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    /// Add a named column payload. Column encoding is the caller's responsibility.
    pub fn add_column(&mut self, name: impl Into<String>, data: Vec<u8>) {
        self.columns.push((name.into(), data));
    }

    /// Serialize the segment to any `Write` destination.
    ///
    /// Format: `[MURR magic][version u32 LE][payload...][footer entries...][footer_size u32 LE]`
    pub fn write(&self, w: &mut impl Write) -> Result<(), MurrError> {
        // Header
        w.write_all(MAGIC)?;
        w.write_all(&VERSION.to_le_bytes())?;

        // Column payloads — track offsets for footer
        let mut offset = HEADER_SIZE as u32;
        let mut entries: Vec<(u32, u32)> = Vec::with_capacity(self.columns.len());
        for (_name, data) in &self.columns {
            w.write_all(data)?;
            let size = data.len() as u32;
            entries.push((offset, size));
            offset += size;
        }

        // Footer entries
        let footer_start = offset;
        for (i, (name, _data)) in self.columns.iter().enumerate() {
            let name_bytes = name.as_bytes();
            w.write_all(&(name_bytes.len() as u16).to_le_bytes())?;
            w.write_all(name_bytes)?;
            let (payload_offset, payload_size) = entries[i];
            w.write_all(&payload_offset.to_le_bytes())?;
            w.write_all(&payload_size.to_le_bytes())?;
        }

        // Footer size (excludes itself)
        let footer_size = offset
            + self
                .columns
                .iter()
                .map(|(name, _)| 2 + name.len() as u32 + 4 + 4)
                .sum::<u32>()
            - footer_start;
        w.write_all(&footer_size.to_le_bytes())?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::format::{FOOTER_LEN_SIZE, HEADER_SIZE};

    #[test]
    fn test_empty_segment() {
        let seg = WriteSegment::new();
        let mut buf = Vec::new();
        seg.write(&mut buf).unwrap();

        // MURR + version + footer_size(0)
        assert_eq!(buf.len(), HEADER_SIZE + FOOTER_LEN_SIZE);
        assert_eq!(&buf[0..4], b"MURR");
        assert_eq!(u32::from_le_bytes(buf[4..8].try_into().unwrap()), 1);
        assert_eq!(u32::from_le_bytes(buf[8..12].try_into().unwrap()), 0);
    }

    #[test]
    fn test_single_column_layout() {
        let mut seg = WriteSegment::new();
        seg.add_column("col1", vec![0xAA, 0xBB, 0xCC]);
        let mut buf = Vec::new();
        seg.write(&mut buf).unwrap();

        // Header
        assert_eq!(&buf[0..4], b"MURR");
        assert_eq!(u32::from_le_bytes(buf[4..8].try_into().unwrap()), 1);

        // Payload at offset 8
        assert_eq!(&buf[8..11], &[0xAA, 0xBB, 0xCC]);

        // Footer entry: name_len(2) + "col1"(4) + offset(4) + size(4) = 14 bytes
        let footer_size_offset = buf.len() - 4;
        let footer_size =
            u32::from_le_bytes(buf[footer_size_offset..].try_into().unwrap()) as usize;
        assert_eq!(footer_size, 2 + 4 + 4 + 4); // 14

        let footer_start = footer_size_offset - footer_size;
        let name_len =
            u16::from_le_bytes(buf[footer_start..footer_start + 2].try_into().unwrap()) as usize;
        assert_eq!(name_len, 4);
        assert_eq!(&buf[footer_start + 2..footer_start + 6], b"col1");

        let payload_offset =
            u32::from_le_bytes(buf[footer_start + 6..footer_start + 10].try_into().unwrap());
        let payload_size = u32::from_le_bytes(
            buf[footer_start + 10..footer_start + 14]
                .try_into()
                .unwrap(),
        );
        assert_eq!(payload_offset, HEADER_SIZE as u32);
        assert_eq!(payload_size, 3);
    }
}
