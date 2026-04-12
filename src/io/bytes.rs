pub trait FromBytes<T> {
    fn from_bytes(page: &[u8], page_offset: u32, size: u32) -> T;
}

impl FromBytes<f32> for f32 {
    fn from_bytes(page: &[u8], page_offset: u32, _size: u32) -> f32 {
        let off = page_offset as usize;
        f32::from_le_bytes(page[off..off + 4].try_into().unwrap())
    }
}

impl FromBytes<f64> for f64 {
    fn from_bytes(page: &[u8], page_offset: u32, _size: u32) -> f64 {
        let off = page_offset as usize;
        f64::from_le_bytes(page[off..off + 8].try_into().unwrap())
    }
}

impl FromBytes<String> for String {
    fn from_bytes(page: &[u8], page_offset: u32, size: u32) -> String {
        let start = page_offset as usize;
        let end = start + (size as usize);
        String::from_utf8(page[start..end].to_vec()).expect("invalid UTF-8 in segment data")
    }
}

impl FromBytes<u64> for u64 {
    fn from_bytes(page: &[u8], page_offset: u32, _size: u32) -> u64 {
        let off = page_offset as usize;
        u64::from_le_bytes(page[off..off + 8].try_into().unwrap())
    }
}

impl FromBytes<Vec<u8>> for Vec<u8> {
    fn from_bytes(page: &[u8], page_offset: u32, size: u32) -> Vec<u8> {
        let start = page_offset as usize;
        let end = start + (size as usize);
        page[start..end].to_vec()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StringOffsetPair {
    pub start: i32,
    pub end: i32,
}

impl FromBytes<StringOffsetPair> for StringOffsetPair {
    fn from_bytes(page: &[u8], page_offset: u32, _size: u32) -> StringOffsetPair {
        let off = page_offset as usize;
        StringOffsetPair {
            start: i32::from_le_bytes(page[off..off + 4].try_into().unwrap()),
            end: i32::from_le_bytes(page[off + 4..off + 8].try_into().unwrap()),
        }
    }
}
