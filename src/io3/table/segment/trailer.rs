use crate::core::MurrError;

pub struct SegmentTrailer {
    pub version: u32,
    pub footer_size: u32,
}

impl SegmentTrailer {
    pub const SIZE: usize = 8;

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut out = [0u8; Self::SIZE];
        out[0..4].copy_from_slice(&self.footer_size.to_le_bytes());
        out[4..8].copy_from_slice(&self.version.to_le_bytes());
        out
    }

    pub fn from_tail(bytes: &[u8]) -> Result<Self, MurrError> {
        if bytes.len() < Self::SIZE {
            return Err(MurrError::SegmentError(format!(
                "segment tail too short: {} bytes, need {}",
                bytes.len(),
                Self::SIZE
            )));
        }
        let n = bytes.len();
        let footer_size = u32::from_le_bytes(bytes[n - 8..n - 4].try_into().unwrap());
        let version = u32::from_le_bytes(bytes[n - 4..n].try_into().unwrap());
        Ok(SegmentTrailer {
            version,
            footer_size,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let trailer = SegmentTrailer {
            version: 7,
            footer_size: 0xDEAD_BEEF,
        };
        let bytes = trailer.to_bytes();
        let back = SegmentTrailer::from_tail(&bytes).unwrap();
        assert_eq!(back.version, trailer.version);
        assert_eq!(back.footer_size, trailer.footer_size);
    }

    #[test]
    fn from_tail_reads_only_last_8_bytes() {
        let trailer = SegmentTrailer {
            version: 1,
            footer_size: 42,
        };
        let mut buf = vec![0xAAu8; 1024];
        buf.extend_from_slice(&trailer.to_bytes());
        let back = SegmentTrailer::from_tail(&buf).unwrap();
        assert_eq!(back.version, 1);
        assert_eq!(back.footer_size, 42);
    }

    #[test]
    fn from_tail_too_short() {
        assert!(SegmentTrailer::from_tail(&[0u8; 7]).is_err());
    }
}