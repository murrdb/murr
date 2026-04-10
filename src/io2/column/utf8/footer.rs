use crate::core::MurrError;
use crate::io2::column::{ColumnFooter, OffsetSize};

// Footer layout (from end of data):
//   [offsets_offset:u32][offsets_size:u32][payload_offset:u32][payload_size:u32]
//   [bitmap_offset:u32][bitmap_size:u32][version:u32][footer_len:u32]
pub const FOOTER_VERSION: u32 = 1;
const FOOTER_BODY_SIZE: usize = 24; // 6 fields * 4 bytes
const FOOTER_TOTAL_SIZE: usize = FOOTER_BODY_SIZE + 4 + 4; // body + version + footer_len

#[derive(Debug, Clone)]
pub struct Utf8ColumnFooter {
    pub base_offset: u32,
    pub offsets: OffsetSize,
    pub payload: OffsetSize,
    pub bitmap: OffsetSize,
}

fn read_u32(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
}

impl ColumnFooter for Utf8ColumnFooter {
    fn base_offset(&self) -> u32 {
        self.base_offset
    }

    fn bitmap(&self) -> &OffsetSize {
        &self.bitmap
    }

    fn parse(data: &[u8], base_offset: u32) -> Result<Self, MurrError> {
        if data.len() < FOOTER_TOTAL_SIZE {
            return Err(MurrError::SegmentError(
                "utf8 footer: data too short".into(),
            ));
        }
        let body_start = data.len() - FOOTER_TOTAL_SIZE;
        let offsets_offset = read_u32(data, body_start);
        let offsets_size = read_u32(data, body_start + 4);
        let payload_offset = read_u32(data, body_start + 8);
        let payload_size = read_u32(data, body_start + 12);
        let bitmap_offset = read_u32(data, body_start + 16);
        let bitmap_size = read_u32(data, body_start + 20);

        Ok(Utf8ColumnFooter {
            base_offset,
            offsets: OffsetSize {
                offset: offsets_offset + base_offset,
                size: offsets_size,
            },
            payload: OffsetSize {
                offset: payload_offset + base_offset,
                size: payload_size,
            },
            bitmap: if bitmap_size > 0 {
                OffsetSize {
                    offset: bitmap_offset + base_offset,
                    size: bitmap_size,
                }
            } else {
                OffsetSize {
                    offset: 0,
                    size: 0,
                }
            },
        })
    }
}

pub fn encode_footer(footer: &Utf8ColumnFooter) -> Vec<u8> {
    let mut buf = Vec::with_capacity(FOOTER_TOTAL_SIZE);
    buf.extend_from_slice(&footer.offsets.offset.to_le_bytes());
    buf.extend_from_slice(&footer.offsets.size.to_le_bytes());
    buf.extend_from_slice(&footer.payload.offset.to_le_bytes());
    buf.extend_from_slice(&footer.payload.size.to_le_bytes());
    buf.extend_from_slice(&footer.bitmap.offset.to_le_bytes());
    buf.extend_from_slice(&footer.bitmap.size.to_le_bytes());
    buf.extend_from_slice(&FOOTER_VERSION.to_le_bytes());
    let footer_len = (FOOTER_BODY_SIZE + 4) as u32; // body + version
    buf.extend_from_slice(&footer_len.to_le_bytes());
    buf
}

pub fn align8_padding(len: u32) -> u32 {
    (8 - (len % 8)) % 8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footer_roundtrip() {
        let footer = Utf8ColumnFooter {
            base_offset: 0,
            offsets: OffsetSize {
                offset: 0,
                size: 40,
            },
            payload: OffsetSize {
                offset: 40,
                size: 200,
            },
            bitmap: OffsetSize {
                offset: 240,
                size: 8,
            },
        };
        let bytes = encode_footer(&footer);
        let decoded = Utf8ColumnFooter::parse(&bytes, 0).unwrap();
        assert_eq!(decoded.offsets.offset, 0);
        assert_eq!(decoded.offsets.size, 40);
        assert_eq!(decoded.payload.offset, 40);
        assert_eq!(decoded.payload.size, 200);
        assert_eq!(decoded.bitmap.offset, 240);
        assert_eq!(decoded.bitmap.size, 8);
    }

    #[test]
    fn footer_roundtrip_no_bitmap() {
        let footer = Utf8ColumnFooter {
            base_offset: 0,
            offsets: OffsetSize {
                offset: 0,
                size: 12,
            },
            payload: OffsetSize {
                offset: 16,
                size: 30,
            },
            bitmap: OffsetSize {
                offset: 0,
                size: 0,
            },
        };
        let bytes = encode_footer(&footer);
        let decoded = Utf8ColumnFooter::parse(&bytes, 0).unwrap();
        assert_eq!(decoded.offsets.size, 12);
        assert_eq!(decoded.payload.offset, 16);
        assert_eq!(decoded.payload.size, 30);
        assert_eq!(decoded.bitmap.size, 0);
    }

    #[test]
    fn footer_roundtrip_with_base_offset() {
        let footer = Utf8ColumnFooter {
            base_offset: 0,
            offsets: OffsetSize {
                offset: 0,
                size: 40,
            },
            payload: OffsetSize {
                offset: 40,
                size: 200,
            },
            bitmap: OffsetSize {
                offset: 240,
                size: 8,
            },
        };
        let bytes = encode_footer(&footer);
        let decoded = Utf8ColumnFooter::parse(&bytes, 500).unwrap();
        assert_eq!(decoded.base_offset, 500);
        assert_eq!(decoded.offsets.offset, 500);
        assert_eq!(decoded.payload.offset, 540);
        assert_eq!(decoded.bitmap.offset, 740);
    }
}
