use std::sync::{Arc, RwLock};

use arrow::array::RecordBatch;

use crate::{
    core::{MurrError, TableSchema},
    io::{
        batch::{ColumnBatch, RowBatch},
        directory::{DirectoryReader, ReadRequest, SegmentReadRequest},
        info::SegmentInfo,
        model::SegmentSchema,
        row::Row,
        table::{
            index::{KeyIndex, keys::SegmentKeyBytes},
            segment::Segment,
        },
    },
};

const FOOTER_TAIL_SIZE: u32 = 64 * 1024;

pub struct TableReader<R: DirectoryReader> {
    schema: TableSchema,
    segment_schema: SegmentSchema,
    reader: Arc<R>,
    segments: Vec<Option<Segment>>,
    index: RwLock<KeyIndex>,
}

impl<R: DirectoryReader> TableReader<R> {
    pub async fn open(schema: TableSchema, reader: Arc<R>) -> Result<Self, MurrError> {
        let segment_schema = SegmentSchema::from(&schema);
        let infos: Vec<SegmentInfo> = reader.info().segments.clone();

        let mut me = TableReader {
            schema,
            segment_schema,
            reader,
            segments: Vec::new(),
            index: RwLock::new(KeyIndex::empty()),
        };
        me.load_segments(&infos).await?;
        let mut index = KeyIndex::empty();
        me.add_segments_to_index(&infos, &mut index).await?;
        me.index = RwLock::new(index);
        Ok(me)
    }

    pub async fn reopen(self) -> Result<Self, MurrError> {
        let new_reader = Arc::new(self.reader.reopen_reader().await?);
        let new_infos: Vec<SegmentInfo> = new_reader.info().segments.clone();
        let new_ids: hashbrown::HashSet<u32> = new_infos.iter().map(|s| s.id).collect();

        let mut me = TableReader {
            schema: self.schema,
            segment_schema: self.segment_schema,
            reader: new_reader,
            segments: self.segments,
            index: self.index,
        };

        let mut removed: Vec<u32> = Vec::new();
        for (id, slot) in me.segments.iter_mut().enumerate() {
            if slot.is_some() && !new_ids.contains(&(id as u32)) {
                *slot = None;
                removed.push(id as u32);
            }
        }

        let added: Vec<SegmentInfo> = new_infos
            .iter()
            .filter(|info| {
                me.segments
                    .get(info.id as usize)
                    .map(|slot| slot.is_none())
                    .unwrap_or(true)
            })
            .cloned()
            .collect();

        me.load_segments(&added).await?;

        let mut index = std::mem::replace(&mut me.index, RwLock::new(KeyIndex::empty()))
            .into_inner()
            .map_err(|e| MurrError::TableError(format!("index lock poisoned: {e}")))?;
        if !removed.is_empty() {
            index.prune_segments(&removed);
        }
        me.add_segments_to_index(&added, &mut index).await?;
        me.index = RwLock::new(index);

        Ok(me)
    }

    pub async fn read(&self, keys: &[&str], columns: &[&str]) -> Result<RecordBatch, MurrError> {
        let locations = self
            .index
            .read()
            .map_err(|e| MurrError::TableError(format!("index lock poisoned: {e}")))?
            .get(keys);

        let mut requests: Vec<SegmentReadRequest> = Vec::with_capacity(locations.len());
        let mut request_to_key_idx: Vec<usize> = Vec::with_capacity(locations.len());
        for (i, loc_opt) in locations.iter().enumerate() {
            if let Some(loc) = loc_opt {
                let segment = self
                    .segments
                    .get(loc.segment_id as usize)
                    .and_then(|s| s.as_ref())
                    .ok_or_else(|| {
                        MurrError::SegmentError(format!(
                            "segment {} present in index but not loaded",
                            loc.segment_id
                        ))
                    })?;
                let rows_offset = segment.footer().rows.offset;
                requests.push(SegmentReadRequest {
                    segment: loc.segment_id,
                    read: ReadRequest {
                        offset: rows_offset + loc.offset + 4,
                        size: loc.size,
                    },
                });
                request_to_key_idx.push(i);
            }
        }

        let mut rows: Vec<Row> = (0..keys.len())
            .map(|_| Row::all_null(&self.segment_schema))
            .collect();

        if !requests.is_empty() {
            let responses = self.reader.read(&requests).await?;
            for (response, key_idx) in responses.into_iter().zip(request_to_key_idx.into_iter()) {
                rows[key_idx] = Row {
                    bytes: response.bytes,
                };
            }
        }

        let column_batch: ColumnBatch = RowBatch {
            schema: self.segment_schema.clone(),
            rows,
        }
        .try_into()?;
        let record_batch: RecordBatch = column_batch.try_into()?;

        let indexes: Vec<usize> = columns
            .iter()
            .map(|name| {
                record_batch
                    .schema()
                    .index_of(name)
                    .map_err(|e| MurrError::ArrowError(e.to_string()))
            })
            .collect::<Result<_, _>>()?;
        record_batch
            .project(&indexes)
            .map_err(|e| MurrError::ArrowError(e.to_string()))
    }

    async fn load_segments(&mut self, infos: &[SegmentInfo]) -> Result<(), MurrError> {
        if infos.is_empty() {
            return Ok(());
        }
        let footer_requests: Vec<SegmentReadRequest> = infos
            .iter()
            .map(|info| {
                let tail = info.size_bytes.min(FOOTER_TAIL_SIZE);
                SegmentReadRequest {
                    segment: info.id,
                    read: ReadRequest {
                        offset: info.size_bytes - tail,
                        size: tail,
                    },
                }
            })
            .collect();
        let footer_responses = self.reader.read(&footer_requests).await?;

        let max_id = infos.iter().map(|info| info.id).max().unwrap() as usize;
        if max_id >= self.segments.len() {
            self.segments.resize_with(max_id + 1, || None);
        }
        for (info, response) in infos.iter().zip(footer_responses.into_iter()) {
            let segment = Segment::load(&response.bytes)?;
            if &segment.footer().schema != &self.segment_schema {
                return Err(MurrError::SegmentError(format!(
                    "segment {} schema does not match table schema",
                    info.id
                )));
            }
            self.segments[info.id as usize] = Some(segment);
        }
        Ok(())
    }

    async fn add_segments_to_index(
        &self,
        infos: &[SegmentInfo],
        index: &mut KeyIndex,
    ) -> Result<(), MurrError> {
        if infos.is_empty() {
            return Ok(());
        }
        let requests: Vec<SegmentReadRequest> = infos
            .iter()
            .map(|info| {
                let segment = self.segments[info.id as usize]
                    .as_ref()
                    .expect("segment loaded above");
                SegmentReadRequest {
                    segment: info.id,
                    read: ReadRequest {
                        offset: segment.footer().keys.offset,
                        size: segment.footer().keys.size,
                    },
                }
            })
            .collect();
        let responses = self.reader.read(&requests).await?;
        for (info, response) in infos.iter().zip(responses.into_iter()) {
            let key_bytes = SegmentKeyBytes {
                bytes: response.bytes,
            };
            index.add_segment(info.id, &key_bytes);
        }
        Ok(())
    }
}
