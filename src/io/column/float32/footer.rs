pub use crate::io::column::scalar::ScalarColumnFooter as Float32ColumnFooter;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::column::{ColumnFooter, OffsetSize};

    #[test]
    fn footer_roundtrip() {
        let footer = Float32ColumnFooter {
            payload: OffsetSize {
                offset: 0,
                size: 400,
            },
            bitmap: OffsetSize {
                offset: 400,
                size: 8,
            },
        };
        let bytes = footer.encode();
        let decoded = Float32ColumnFooter::parse(&bytes, 0).unwrap();
        assert_eq!(decoded.payload.offset, 0);
        assert_eq!(decoded.payload.size, 400);
        assert_eq!(decoded.bitmap.offset, 400);
        assert_eq!(decoded.bitmap.size, 8);
    }

    #[test]
    fn footer_roundtrip_no_bitmap() {
        let footer = Float32ColumnFooter {
            payload: OffsetSize {
                offset: 0,
                size: 12,
            },
            bitmap: OffsetSize { offset: 0, size: 0 },
        };
        let bytes = footer.encode();
        let decoded = Float32ColumnFooter::parse(&bytes, 0).unwrap();
        assert_eq!(decoded.payload.size, 12);
        assert_eq!(decoded.bitmap.size, 0);
    }

    #[test]
    fn footer_roundtrip_with_base_offset() {
        let footer = Float32ColumnFooter {
            payload: OffsetSize {
                offset: 0,
                size: 400,
            },
            bitmap: OffsetSize {
                offset: 400,
                size: 8,
            },
        };
        let bytes = footer.encode();
        let decoded = Float32ColumnFooter::parse(&bytes, 1000).unwrap();
        assert_eq!(decoded.payload.offset, 1000);
        assert_eq!(decoded.payload.size, 400);
        assert_eq!(decoded.bitmap.offset, 1400);
        assert_eq!(decoded.bitmap.size, 8);
    }
}
