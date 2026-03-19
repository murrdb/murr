use std::collections::HashSet;
use std::path::Path;

use crate::core::MurrError;
use crate::io::directory::SegmentInfo;
use crate::io::segment::Segment;

pub struct TableView {
    segments: Vec<Option<Segment>>,
}

impl TableView {
    pub fn open(
        path: &Path,
        segment_infos: &[SegmentInfo],
        existing: Vec<Segment>,
    ) -> Result<Self, MurrError> {
        let needed_ids: HashSet<u32> = segment_infos.iter().map(|i| i.id).collect();
        let existing_ids: HashSet<u32> = existing.iter().map(|s| s.id()).collect();

        let mut loaded: Vec<Segment> = existing
            .into_iter()
            .filter(|s| needed_ids.contains(&s.id()))
            .collect();

        for info in segment_infos {
            if !existing_ids.contains(&info.id) {
                loaded.push(Segment::open(path.join(&info.file_name))?);
            }
        }

        let max_id = loaded.iter().map(|s| s.id()).max();
        let num_slots = max_id.map(|id| id as usize + 1).unwrap_or(0);
        let mut segments: Vec<Option<Segment>> = (0..num_slots).map(|_| None).collect();
        for seg in loaded {
            let id = seg.id() as usize;
            segments[id] = Some(seg);
        }

        Ok(Self { segments })
    }

    pub fn into_segments(self) -> Vec<Segment> {
        self.segments.into_iter().flatten().collect()
    }

    pub fn segments(&self) -> &[Option<Segment>] {
        &self.segments
    }

    pub fn segment(&self, id: u32) -> Option<&Segment> {
        self.segments.get(id as usize).and_then(|s| s.as_ref())
    }

    pub fn segment_ids(&self) -> Vec<u32> {
        self.segments
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.as_ref().map(|_| i as u32))
            .collect()
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
        assert_eq!(view.segment_ids(), vec![0, 1]);
        assert!(view.segment(0).is_some());
        assert!(view.segment(1).is_some());
    }

    #[test]
    fn test_open_reuses_existing_segments() {
        let dir = TempDir::new().unwrap();
        write_seg(dir.path(), 0);
        write_seg(dir.path(), 1);
        write_seg(dir.path(), 2);

        // Open first two segments
        let view = TableView::open(dir.path(), &[seg_info(0), seg_info(1)], Vec::new()).unwrap();
        assert_eq!(view.segment_ids(), vec![0, 1]);

        // Reopen with all three, passing existing segments
        let existing = view.into_segments();
        let view = TableView::open(
            dir.path(),
            &[seg_info(0), seg_info(1), seg_info(2)],
            existing,
        )
        .unwrap();
        assert_eq!(view.segment_ids(), vec![0, 1, 2]);

        // Verify data is readable
        assert_eq!(view.segment(0).unwrap().column("data").unwrap(), &[0]);
        assert_eq!(view.segment(2).unwrap().column("data").unwrap(), &[2]);
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
        assert_eq!(view.segment_ids(), vec![1, 2]);
        assert!(view.segment(0).is_none());
        assert!(view.segment(1).is_some());
        assert!(view.segment(2).is_some());
    }
}
