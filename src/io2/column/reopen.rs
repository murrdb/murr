use std::sync::Arc;

use log::debug;

use crate::core::MurrError;
use crate::io2::bitmap::NullBitmap;
use crate::io2::column::{ColumnFooter, OffsetSize, MAX_COLUMN_HEADER_SIZE};
use crate::io2::directory::{ReadRequest, Reader, SegmentReadRequest};
use crate::io2::info::ColumnSegments;

pub struct OpenedSegments<F: ColumnFooter> {
    pub segments: Vec<Option<F>>,
    pub bitmap: NullBitmap,
}

pub async fn open_segments<F>(
    reader: &Arc<dyn Reader>,
    column: &ColumnSegments,
    prev_segments: Option<&Vec<Option<F>>>,
    prev_bitmap: Option<&NullBitmap>,
) -> Result<OpenedSegments<F>, MurrError>
where
    F: ColumnFooter,
{
    let all_segment_ids: Vec<u32> = column.segments.keys().copied().collect();
    if all_segment_ids.is_empty() {
        debug!("column '{}': no segments to open", column.column.name);
        return Ok(OpenedSegments {
            segments: Vec::new(),
            bitmap: NullBitmap::new(Vec::new(), reader.clone()),
        });
    }

    let max_seg_id = *all_segment_ids.iter().max().unwrap() as usize;
    let mut segments: Vec<Option<F>> = vec![None; max_seg_id + 1];
    let mut bitmap_segments: Vec<Option<OffsetSize>> = vec![None; max_seg_id + 1];

    // Copy footers from previous reader for existing segments
    let new_segment_ids: Vec<u32> = match (prev_segments, prev_bitmap) {
        (Some(prev_segs), Some(prev_bm)) => {
            for &seg_id in &all_segment_ids {
                let idx = seg_id as usize;
                if let Some(footer) = prev_segs.get(idx).and_then(|f| f.as_ref()) {
                    segments[idx] = Some(footer.clone());
                    if let Some(bm) = prev_bm.segments.get(idx).and_then(|b| b.as_ref()) {
                        bitmap_segments[idx] = Some(bm.clone());
                    }
                }
            }
            let new: Vec<u32> = all_segment_ids
                .iter()
                .copied()
                .filter(|&id| {
                    prev_segs
                        .get(id as usize)
                        .and_then(|f| f.as_ref())
                        .is_none()
                })
                .collect();
            debug!(
                "column '{}': reopen with {} total segments, {} new, {} reused",
                column.column.name,
                all_segment_ids.len(),
                new.len(),
                all_segment_ids.len() - new.len()
            );
            new
        }
        _ => {
            debug!(
                "column '{}': fresh open with {} segments",
                column.column.name,
                all_segment_ids.len()
            );
            all_segment_ids
        }
    };

    if !new_segment_ids.is_empty() {
        let requests: Vec<SegmentReadRequest> = new_segment_ids
            .iter()
            .map(|&seg_id| {
                let seg_info = &column.segments[&seg_id];
                let read_size = MAX_COLUMN_HEADER_SIZE.min(seg_info.length);
                let read_offset = seg_info.offset + seg_info.length - read_size;
                SegmentReadRequest {
                    segment: seg_id,
                    read: ReadRequest {
                        offset: read_offset,
                        size: read_size,
                    },
                }
            })
            .collect();

        let raw_footers: Vec<Vec<u8>> = reader.read_bytes(&requests).await?;

        for (i, &seg_id) in new_segment_ids.iter().enumerate() {
            let seg_info = &column.segments[&seg_id];
            let footer = F::parse(&raw_footers[i], seg_info.offset)?;
            if footer.bitmap().size > 0 {
                bitmap_segments[seg_id as usize] = Some(footer.bitmap().clone());
            }
            segments[seg_id as usize] = Some(footer);
        }
    }

    Ok(OpenedSegments {
        segments,
        bitmap: NullBitmap::new(bitmap_segments, reader.clone()),
    })
}
