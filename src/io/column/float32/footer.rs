use crate::core::MurrError;
use crate::io::column::{read_u32, ColumnFooter, OffsetSize};

// Footer layout (from end of data):
//   [payload_offset:u32][payload_size:u32][bitmap_offset:u32][bitmap_size:u32][version:u32][footer_len:u32]
// footer_len includes version (4) + body (16) = 20
pub const FOOTER_VERSION: u32 = 1;
const FOOTER_BODY_SIZE: usize = 16; // 4 fields * 4 bytes
const FOOTER_TOTAL_SIZE: usize = FOOTER_BODY_SIZE + 4 + 4; // body + version + footer_len

#[derive(Debug, Clone)]
pub struct Float32ColumnFooter {
    pub base_offset: u32,
    pub payload: OffsetSize,
    pub bitmap: OffsetSize,
}

impl ColumnFooter for Float32ColumnFooter {
    fn base_offset(&self) -> u32 {
        self.base_offset
    }

    fn bitmap(&self) -> &OffsetSize {
        &self.bitmap
    }

    fn parse(data: &[u8], base_offset: u32) -> Result<Self, MurrError> {
        if data.len() < FOOTER_TOTAL_SIZE {
            return Err(MurrError::SegmentError(
                "float32 footer: data too short".into(),
            ));
        }
        let body_start = data.len() - FOOTER_TOTAL_SIZE;
        let payload_offset = read_u32(data, body_start);
        let payload_size = read_u32(data, body_start + 4);
        let bitmap_offset = read_u32(data, body_start + 8);
        let bitmap_size = read_u32(data, body_start + 12);

        Ok(Float32ColumnFooter {
            base_offset,
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

pub fn encode_footer(footer: &Float32ColumnFooter) -> Vec<u8> {
    let mut buf = Vec::with_capacity(FOOTER_TOTAL_SIZE);
    buf.extend_from_slice(&footer.payload.offset.to_le_bytes());
    buf.extend_from_slice(&footer.payload.size.to_le_bytes());
    buf.extend_from_slice(&footer.bitmap.offset.to_le_bytes());
    buf.extend_from_slice(&footer.bitmap.size.to_le_bytes());
    buf.extend_from_slice(&FOOTER_VERSION.to_le_bytes());
    let footer_len = (FOOTER_BODY_SIZE + 4) as u32; // body + version
    buf.extend_from_slice(&footer_len.to_le_bytes());
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footer_roundtrip() {
        let footer = Float32ColumnFooter {
            base_offset: 0,
            payload: OffsetSize {
                offset: 0,
                size: 400,
            },
            bitmap: OffsetSize {
                offset: 400,
                size: 8,
            },
        };
        let bytes = encode_footer(&footer);
        let decoded = Float32ColumnFooter::parse(&bytes, 0).unwrap();
        assert_eq!(decoded.payload.offset, 0);
        assert_eq!(decoded.payload.size, 400);
        assert_eq!(decoded.bitmap.offset, 400);
        assert_eq!(decoded.bitmap.size, 8);
    }

    #[test]
    fn footer_roundtrip_no_bitmap() {
        let footer = Float32ColumnFooter {
            base_offset: 0,
            payload: OffsetSize {
                offset: 0,
                size: 12,
            },
            bitmap: OffsetSize {
                offset: 0,
                size: 0,
            },
        };
        let bytes = encode_footer(&footer);
        let decoded = Float32ColumnFooter::parse(&bytes, 0).unwrap();
        assert_eq!(decoded.payload.size, 12);
        assert_eq!(decoded.bitmap.size, 0);
    }

    #[test]
    fn footer_roundtrip_with_base_offset() {
        let footer = Float32ColumnFooter {
            base_offset: 0,
            payload: OffsetSize {
                offset: 0,
                size: 400,
            },
            bitmap: OffsetSize {
                offset: 400,
                size: 8,
            },
        };
        let bytes = encode_footer(&footer);
        let decoded = Float32ColumnFooter::parse(&bytes, 1000).unwrap();
        assert_eq!(decoded.base_offset, 1000);
        assert_eq!(decoded.payload.offset, 1000);
        assert_eq!(decoded.payload.size, 400);
        assert_eq!(decoded.bitmap.offset, 1400);
        assert_eq!(decoded.bitmap.size, 8);
    }
}
