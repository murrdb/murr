use bincode::{Decode, Encode};

use crate::core::MurrError;
use crate::io2::bytes::FromBytes;
use crate::io2::column::OffsetSize;

pub const FOOTER_VERSION: u32 = 1;
const FOOTER_LEN_SIZE: usize = 4;
const FOOTER_VERSION_SIZE: usize = 4;

pub const BINCODE_CONFIG: bincode::config::Configuration<
    bincode::config::LittleEndian,
    bincode::config::Fixint,
> = bincode::config::standard()
    .with_fixed_int_encoding()
    .with_little_endian();

#[derive(Debug, Clone, Encode, Decode)]
pub struct Utf8ColumnFooter {
    pub offsets: OffsetSize,
    pub payload: OffsetSize,
    pub bitmap: OffsetSize,
}

impl FromBytes<Utf8ColumnFooter> for Utf8ColumnFooter {
    fn from_bytes(page: &[u8], page_offset: u32, size: u32) -> Utf8ColumnFooter {
        let start = page_offset as usize;
        let end = start + size as usize;
        let data = &page[start..end];

        // Last 4 bytes: footer_len (includes version bytes)
        let footer_len =
            u32::from_le_bytes(data[data.len() - FOOTER_LEN_SIZE..].try_into().unwrap()) as usize;

        // 4 bytes before that: footer_version
        let version_start = data.len() - FOOTER_LEN_SIZE - FOOTER_VERSION_SIZE;
        let _footer_version =
            u32::from_le_bytes(data[version_start..version_start + 4].try_into().unwrap());

        // bincode footer is footer_len - FOOTER_VERSION_SIZE bytes before the version
        let bincode_len = footer_len - FOOTER_VERSION_SIZE;
        let footer_start = version_start - bincode_len;
        let (footer, _): (Utf8ColumnFooter, _) = bincode::decode_from_slice(
            &data[footer_start..footer_start + bincode_len],
            BINCODE_CONFIG,
        )
        .expect("failed to decode Utf8ColumnFooter");
        footer
    }
}

pub fn encode_footer(footer: &Utf8ColumnFooter) -> Result<Vec<u8>, MurrError> {
    let footer_bytes = bincode::encode_to_vec(footer, BINCODE_CONFIG)
        .map_err(|e| MurrError::SegmentError(format!("encoding utf8 footer: {e}")))?;
    let footer_len = (footer_bytes.len() + FOOTER_VERSION_SIZE) as u32;

    let mut buf = Vec::with_capacity(footer_bytes.len() + FOOTER_LEN_SIZE + FOOTER_VERSION_SIZE);
    buf.extend_from_slice(&footer_bytes);
    buf.extend_from_slice(&FOOTER_VERSION.to_le_bytes());
    buf.extend_from_slice(&footer_len.to_le_bytes());
    Ok(buf)
}

pub fn align8_padding(len: u32) -> u32 {
    (8 - (len % 8)) % 8
}

#[derive(Debug, Clone, Copy)]
pub struct StringOffsetPair {
    pub start: i32,
    pub end: i32,
}

impl FromBytes<StringOffsetPair> for StringOffsetPair {
    fn from_bytes(page: &[u8], page_offset: u32, _size: u32) -> StringOffsetPair {
        unsafe {
            let ptr = page.as_ptr().add(page_offset as usize);
            StringOffsetPair {
                start: *(ptr as *const i32),
                end: *(ptr.add(4) as *const i32),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footer_roundtrip() {
        let footer = Utf8ColumnFooter {
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
        let bytes = encode_footer(&footer).unwrap();
        let decoded = Utf8ColumnFooter::from_bytes(&bytes, 0, bytes.len() as u32);
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
        let bytes = encode_footer(&footer).unwrap();
        let decoded = Utf8ColumnFooter::from_bytes(&bytes, 0, bytes.len() as u32);
        assert_eq!(decoded.offsets.size, 12);
        assert_eq!(decoded.payload.offset, 16);
        assert_eq!(decoded.payload.size, 30);
        assert_eq!(decoded.bitmap.size, 0);
    }
}
