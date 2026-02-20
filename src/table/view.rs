use std::path::Path;

use crate::core::MurrError;
use crate::directory::SegmentInfo;
use crate::segment::Segment;

pub struct TableView {
    segments: Vec<Segment>,
}

impl TableView {
    pub fn open(path: &Path, segment_infos: &[SegmentInfo]) -> Result<Self, MurrError> {
        let segments: Result<Vec<_>, _> = segment_infos
            .iter()
            .map(|info| Segment::open(path.join(&info.file_name)))
            .collect();

        Ok(Self {
            segments: segments?,
        })
    }

    pub fn segments(&self) -> &[Segment] {
        &self.segments
    }
}
