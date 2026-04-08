pub trait FromBytes<T> {
    fn from_bytes(page: &[u8], page_offset: u32, size: u32) -> T;
}

impl FromBytes<f32> for f32 {
    fn from_bytes(page: &[u8], page_offset: u32, _size: u32) -> f32 {
        // yolo cast!
        unsafe { *(page.as_ptr().add(page_offset as usize) as *const f32) }
    }
}

impl FromBytes<String> for String {
    fn from_bytes(page: &[u8], page_offset: u32, size: u32) -> String {
        let start = page_offset as usize;
        let end = start + (size as usize);
        unsafe { String::from_utf8_unchecked(page[start..end].to_vec()) }
    }
}

impl FromBytes<Vec<u8>> for Vec<u8> {
    fn from_bytes(page: &[u8], page_offset: u32, size: u32) -> Vec<u8> {
        let start = page_offset as usize;
        let end = start + (size as usize);
        page[start..end].to_vec()
    }
}
