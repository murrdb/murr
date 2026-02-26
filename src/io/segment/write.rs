use std::io::Write;

use crate::core::MurrError;

use super::format::{
    FooterEntry, HEADER_SIZE, MAGIC, SegmentFooter, VERSION, align8_padding, encode_footer,
};

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
    /// Format: `[MURR magic][version u32 LE][payload...][bincode footer][footer_size u32 LE]`
    pub fn write(&self, w: &mut impl Write) -> Result<(), MurrError> {
        // Header
        w.write_all(MAGIC)?;
        w.write_all(&VERSION.to_le_bytes())?;

        // Column payloads — track offsets for footer.
        // Each column is padded to 8-byte alignment so typed data
        // (f32, i32, u64 via bytemuck::cast_slice) can be read zero-copy.
        let mut offset = HEADER_SIZE as u32;
        let mut entries: Vec<FooterEntry> = Vec::with_capacity(self.columns.len());
        for (name, data) in &self.columns {
            w.write_all(data)?;
            let size = data.len() as u32;
            entries.push(FooterEntry {
                name: name.clone(),
                offset,
                size,
            });
            offset += size;
            let padding = align8_padding(data.len()) as u32;
            if padding > 0 {
                w.write_all(&[0u8; 7][..padding as usize])?;
                offset += padding;
            }
        }

        // Footer: bincode-encoded, then footer byte count as u32 LE
        let footer = SegmentFooter { columns: entries };
        let mut footer_buf = Vec::new();
        encode_footer(&mut footer_buf, &footer)?;
        w.write_all(&footer_buf)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::segment::format::{FOOTER_LEN_SIZE, HEADER_SIZE, decode_footer};

    #[test]
    fn test_empty_segment() {
        let seg = WriteSegment::new();
        let mut buf = Vec::new();
        seg.write(&mut buf).unwrap();

        // MURR + version + bincode footer (empty vec) + footer_size
        assert_eq!(&buf[0..4], b"MURR");
        assert_eq!(u32::from_le_bytes(buf[4..8].try_into().unwrap()), 2);

        let footer: SegmentFooter = decode_footer(&buf[HEADER_SIZE..], "test").unwrap();
        assert!(footer.columns.is_empty());
    }

    #[test]
    fn test_single_column_layout() {
        let mut seg = WriteSegment::new();
        seg.add_column("col1", vec![0xAA, 0xBB, 0xCC]);
        let mut buf = Vec::new();
        seg.write(&mut buf).unwrap();

        // Header
        assert_eq!(&buf[0..4], b"MURR");
        assert_eq!(u32::from_le_bytes(buf[4..8].try_into().unwrap()), 2);

        // Payload at offset 8 (3 bytes + 5 bytes padding to align to 8)
        assert_eq!(&buf[8..11], &[0xAA, 0xBB, 0xCC]);
        // 5 padding bytes to reach 8-byte alignment
        assert_eq!(&buf[11..16], &[0, 0, 0, 0, 0]);

        // Footer
        let footer_size = u32::from_le_bytes(
            buf[buf.len() - FOOTER_LEN_SIZE..].try_into().unwrap(),
        ) as usize;
        let footer_start = buf.len() - FOOTER_LEN_SIZE - footer_size;
        let footer: SegmentFooter = decode_footer(&buf[footer_start..], "test").unwrap();
        assert_eq!(footer.columns.len(), 1);
        assert_eq!(footer.columns[0].name, "col1");
        assert_eq!(footer.columns[0].offset, HEADER_SIZE as u32);
        assert_eq!(footer.columns[0].size, 3);
    }
}
