const MISSING_KEY: u32 = u32::MAX;

pub struct KeyOffset {
    segment: u32,
    index: u32,
}

impl KeyOffset {
    fn new(segment: u32, index: u32) -> Self {
        KeyOffset { segment, index }
    }

    fn missing() -> Self {
        KeyOffset {
            segment: MISSING_KEY,
            index: MISSING_KEY,
        }
    }

    fn is_missing(&self) -> bool {
        return self.segment == MISSING_KEY;
    }
}
