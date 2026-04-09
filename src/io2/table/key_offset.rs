const MISSING_KEY: u32 = u32::MAX;

pub struct KeyOffset {
    pub request_index: usize,
    pub segment: u32,
    pub segment_index: u32,
}

impl KeyOffset {
    fn new(request_index: usize, segment: u32, segment_index: u32) -> Self {
        KeyOffset {
            request_index,
            segment,
            segment_index,
        }
    }

    fn missing(request_index: usize) -> Self {
        KeyOffset {
            request_index: request_index,
            segment: MISSING_KEY,
            segment_index: MISSING_KEY,
        }
    }

    fn is_missing(&self) -> bool {
        return self.segment == MISSING_KEY;
    }
}
