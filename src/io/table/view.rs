use std::collections::HashSet;
use std::path::Path;

use crate::core::MurrError;
use crate::io::directory::SegmentInfo;
use crate::io::segment::Segment;

pub struct TableView {
    segments: Vec<Segment>,
}

impl TableView {
    pub fn open(
        path: &Path,
        segment_infos: &[SegmentInfo],
        existing: Vec<Segment>,
    ) -> Result<Self, MurrError> {
        let needed_ids: HashSet<u32> = segment_infos.iter().map(|i| i.id).collect();
        let existing_ids: HashSet<u32> = existing.iter().map(|s| s.id()).collect();

        let mut segments: Vec<Segment> = existing
            .into_iter()
            .filter(|s| needed_ids.contains(&s.id()))
            .collect();

        for info in segment_infos {
            if !existing_ids.contains(&info.id) {
                segments.push(Segment::open(path.join(&info.file_name))?);
            }
        }

        segments.sort_by_key(|s| s.id());

        Ok(Self { segments })
    }

    pub fn into_segments(self) -> Vec<Segment> {
        self.segments
    }

    pub fn segments(&self) -> &[Segment] {
        &self.segments
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::segment::WriteSegment;
    use std::fs::File;
    use std::time::SystemTime;
    use tempfile::TempDir;

    fn write_seg(dir: &std::path::Path, id: u32) {
        let mut ws = WriteSegment::new();
        ws.add_column("data", vec![id as u8]);
        let path = dir.join(format!("{:08}.seg", id));
        let mut file = File::create(&path).unwrap();
        ws.write(&mut file).unwrap();
    }

    fn seg_info(id: u32) -> SegmentInfo {
        SegmentInfo {
            id,
            size: 0,
            file_name: format!("{:08}.seg", id),
            last_modified: SystemTime::now(),
        }
    }

    #[test]
    fn test_open_fresh() {
        let dir = TempDir::new().unwrap();
        write_seg(dir.path(), 0);
        write_seg(dir.path(), 1);

        let view = TableView::open(dir.path(), &[seg_info(0), seg_info(1)], Vec::new()).unwrap();
        assert_eq!(view.segments().len(), 2);
        assert_eq!(view.segments()[0].id(), 0);
        assert_eq!(view.segments()[1].id(), 1);
    }

    #[test]
    fn test_open_reuses_existing_segments() {
        let dir = TempDir::new().unwrap();
        write_seg(dir.path(), 0);
        write_seg(dir.path(), 1);
        write_seg(dir.path(), 2);

        // Open first two segments
        let view = TableView::open(dir.path(), &[seg_info(0), seg_info(1)], Vec::new()).unwrap();
        assert_eq!(view.segments().len(), 2);

        // Reopen with all three, passing existing segments
        let existing = view.into_segments();
        let view = TableView::open(
            dir.path(),
            &[seg_info(0), seg_info(1), seg_info(2)],
            existing,
        )
        .unwrap();
        assert_eq!(view.segments().len(), 3);
        assert_eq!(view.segments()[0].id(), 0);
        assert_eq!(view.segments()[1].id(), 1);
        assert_eq!(view.segments()[2].id(), 2);

        // Verify data is readable
        assert_eq!(view.segments()[0].column("data").unwrap(), &[0]);
        assert_eq!(view.segments()[2].column("data").unwrap(), &[2]);
    }

    #[test]
    fn test_open_filters_removed_segments() {
        let dir = TempDir::new().unwrap();
        write_seg(dir.path(), 0);
        write_seg(dir.path(), 1);
        write_seg(dir.path(), 2);

        let view = TableView::open(
            dir.path(),
            &[seg_info(0), seg_info(1), seg_info(2)],
            Vec::new(),
        )
        .unwrap();

        // Reopen with only segment 1 and 2 needed (simulating compaction)
        let existing = view.into_segments();
        let view = TableView::open(
            dir.path(),
            &[seg_info(1), seg_info(2)],
            existing,
        )
        .unwrap();
        assert_eq!(view.segments().len(), 2);
        assert_eq!(view.segments()[0].id(), 1);
        assert_eq!(view.segments()[1].id(), 2);
    }
}
