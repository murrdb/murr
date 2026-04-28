use serde::{Deserialize, Serialize};

use crate::{
    core::MurrError,
    io3::{
        model::{OffsetSize, SegmentSchema},
        table::segment::trailer::SegmentTrailer,
    },
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentFooterV1 {
    pub row_count: u32,
    pub schema: SegmentSchema,
    pub keys: OffsetSize,
    pub rows: OffsetSize,
}

impl SegmentFooterV1 {
    pub const SEGMENT_FOOTER_VERSION: u32 = 1;
    pub fn from_last_block(bytes: &[u8]) -> Result<SegmentFooterV1, MurrError> {
        let trailer = SegmentTrailer::from_tail(bytes)?;
        if trailer.version != SegmentFooterV1::SEGMENT_FOOTER_VERSION {
            return Err(MurrError::SegmentError(format!(
                "unsupported segment footer version: {}, expected {}",
                trailer.version,
                SegmentFooterV1::SEGMENT_FOOTER_VERSION
            )));
        }
        let footer_size = trailer.footer_size as usize;
        let n = bytes.len();
        if n < SegmentTrailer::SIZE + footer_size {
            return Err(MurrError::SegmentError(format!(
                "segment tail too short for declared footer_size {footer_size}"
            )));
        }
        let footer_slice = &bytes[n - SegmentTrailer::SIZE - footer_size..n - SegmentTrailer::SIZE];
        let (footer, _) = bincode::serde::decode_from_slice::<SegmentFooterV1, _>(
            footer_slice,
            bincode::config::standard(),
        )
        .map_err(|e| MurrError::SegmentError(format!("footer decode failed: {e}")))?;
        Ok(footer)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, MurrError> {
        let payload = bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| MurrError::SegmentError(format!("footer encode failed: {e}")))?;
        let trailer = SegmentTrailer {
            version: SegmentFooterV1::SEGMENT_FOOTER_VERSION,
            footer_size: payload.len() as u32,
        };
        let mut buf = Vec::with_capacity(payload.len() + SegmentTrailer::SIZE);
        buf.extend_from_slice(&payload);
        buf.extend_from_slice(&trailer.to_bytes());
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{core::DType, io3::model::SegmentColumnSchema};

    fn sample_footer() -> SegmentFooterV1 {
        let columns = vec![
            SegmentColumnSchema {
                index: 0,
                dtype: DType::Utf8,
                name: "id".into(),
                offset: 0,
            },
            SegmentColumnSchema {
                index: 1,
                dtype: DType::Float32,
                name: "score".into(),
                offset: 4,
            },
        ];
        SegmentFooterV1 {
            row_count: 17,
            schema: SegmentSchema::new(&columns),
            rows: OffsetSize {
                offset: 0,
                size: 1024,
            },
            keys: OffsetSize {
                offset: 1024,
                size: 256,
            },
        }
    }

    #[test]
    fn roundtrip() {
        let footer = sample_footer();
        let bytes = footer.to_bytes().unwrap();
        let back = SegmentFooterV1::from_last_block(&bytes).unwrap();
        assert_eq!(back, footer);
    }
}
