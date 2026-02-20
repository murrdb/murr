pub(crate) const MAGIC: &[u8; 4] = b"MURR";
pub(crate) const VERSION: u32 = 1;
pub(crate) const HEADER_SIZE: usize = 8; // magic (4) + version (4)
pub(crate) const FOOTER_LEN_SIZE: usize = 4; // trailing u32 footer length

/// Read a little-endian u32 from `data` at `offset`.
/// Caller must ensure `offset + 4 <= data.len()`.
pub(crate) fn read_u32_le(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
}

/// Read a little-endian u16 from `data` at `offset`.
/// Caller must ensure `offset + 2 <= data.len()`.
pub(crate) fn read_u16_le(data: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap())
}
