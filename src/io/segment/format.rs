use bincode::{Decode, Encode};

use crate::core::MurrError;

pub(crate) const MAGIC: &[u8; 4] = b"MURR";
pub(crate) const VERSION: u32 = 2;
pub(crate) const HEADER_SIZE: usize = 8; // magic (4) + version (4)
pub(crate) const FOOTER_LEN_SIZE: usize = 4; // trailing u32 footer length

/// Bincode configuration used for all footer encoding/decoding.
/// Uses little-endian byte order and fixed-width integers for deterministic
/// wire format sizes.
pub(crate) const BINCODE_CONFIG: bincode::config::Configuration<
    bincode::config::LittleEndian,
    bincode::config::Fixint,
> = bincode::config::standard()
    .with_fixed_int_encoding()
    .with_little_endian();

/// Number of padding bytes needed to align `len` up to 8 bytes.
pub(crate) fn align8_padding(len: usize) -> usize {
    (8 - (len % 8)) % 8
}

/// Marker trait for types that follow the "bincode footer + u32 LE length
/// suffix" wire convention. All footers (segment-level and column-level) are
/// serialized at the end of their data region, followed by a 4-byte LE length.
pub(crate) trait Footer: Encode + Decode<()> {}

/// Decode a footer from the tail of a byte slice.
///
/// Expects the last 4 bytes to be a LE u32 footer length, with the
/// bincode-encoded footer immediately before it.
pub(crate) fn decode_footer<T: Footer>(data: &[u8], label: &str) -> Result<T, MurrError> {
    if data.len() < FOOTER_LEN_SIZE {
        return Err(MurrError::SegmentError(format!(
            "{label} too small for footer length"
        )));
    }
    let footer_len = u32::from_le_bytes(
        data[data.len() - FOOTER_LEN_SIZE..]
            .try_into()
            .map_err(|_| MurrError::SegmentError(format!("{label} footer length read failed")))?,
    ) as usize;
    let footer_end = data.len() - FOOTER_LEN_SIZE;
    if footer_len > footer_end {
        return Err(MurrError::SegmentError(format!(
            "{label} footer length {footer_len} exceeds available data {footer_end}"
        )));
    }
    let footer_start = footer_end - footer_len;
    let (footer, _): (T, _) =
        bincode::decode_from_slice(&data[footer_start..footer_end], BINCODE_CONFIG)
            .map_err(|e| MurrError::SegmentError(format!("decoding {label} footer: {e}")))?;
    Ok(footer)
}

/// Encode a footer and append it plus its LE u32 length to a buffer.
pub(crate) fn encode_footer(buf: &mut Vec<u8>, footer: &impl Footer) -> Result<(), MurrError> {
    let footer_bytes = bincode::encode_to_vec(footer, BINCODE_CONFIG)
        .map_err(|e| MurrError::SegmentError(format!("encoding footer: {e}")))?;
    buf.extend_from_slice(&footer_bytes);
    buf.extend_from_slice(&(footer_bytes.len() as u32).to_le_bytes());
    Ok(())
}

/// A single column entry in the segment footer.
#[derive(Debug, Clone, Encode, Decode)]
pub(crate) struct FooterEntry {
    pub name: String,
    pub offset: u32,
    pub size: u32,
}

/// The segment-level footer, listing all columns and their byte ranges.
#[derive(Debug, Clone, Encode, Decode)]
pub(crate) struct SegmentFooter {
    pub columns: Vec<FooterEntry>,
}

impl Footer for SegmentFooter {}
