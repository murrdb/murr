use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, StringArray, UInt32Array};
use arrow::buffer::Buffer;
use arrow::compute::take;
use arrow::datatypes::Schema;
use arrow::ipc::convert::fb_to_schema;
use arrow::ipc::reader::{FileDecoder, read_footer_length};
use arrow::ipc::root_as_footer;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use memmap2::Mmap;

use crate::core::MurrError;

pub type KeyIndex = HashMap<String, (u32, u32)>; // (batch_index, row_offset)

const TRAILER_SIZE: usize = 10;
const ARROW_MAGIC: &[u8; 6] = b"ARROW1";

pub struct Table {
    _buffer: Buffer,
    batches: Vec<RecordBatch>,
    index: KeyIndex,
}

impl Table {
    pub fn open<P: AsRef<Path>>(path: P, key_column: &str) -> Result<Self, MurrError> {
        // zero-copy mmap the file
        let file = File::open(path.as_ref())?;
        let mmap = unsafe { Mmap::map(&file)? };
        let bytes = Bytes::from_owner(mmap);
        let buffer = Buffer::from(bytes);

        // read the footer
        let trailer_start = buffer.len() - TRAILER_SIZE;
        let footer_len = read_footer_length(buffer[trailer_start..].try_into().unwrap())
            .map_err(|e| MurrError::ArrowError(e.to_string()))?;
        let footer = root_as_footer(&buffer[trailer_start - footer_len..trailer_start])
            .map_err(|e| MurrError::ArrowError(e.to_string()))?;

        // read schema
        let schema =
            fb_to_schema(footer.schema().ok_or_else(|| {
                MurrError::TableError("Missing schema in IPC footer".to_string())
            })?);
        let mut decoder = FileDecoder::new(Arc::new(schema), footer.version());

        // for dic-encoded columns (like utf8) we had to load them first
        // the decoder is mutable, dic mapping is stored inside it

        for block in footer.dictionaries().iter().flatten() {
            let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
            let data = buffer.slice_with_length(block.offset() as usize, block_len);
            decoder
                .read_dictionary(block, &data)
                .map_err(|e| MurrError::ArrowError(e.to_string()))?;
        }

        // read all batches
        let mut batches: Vec<RecordBatch> = Vec::new();
        for block in footer.recordBatches().iter().flatten() {
            let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
            let data = buffer.slice_with_length(block.offset() as usize, block_len);
            let batch = decoder
                .read_record_batch(&block, &data)
                .map_err(|e| MurrError::ArrowError(e.to_string()))?
                .ok_or_else(|| MurrError::TableError("Failed to decode batch".to_string()))?;
            batches.push(batch);
        }

        if batches.is_empty() {
            return Err(MurrError::TableError(
                "No record batches in arrow file!".to_string(),
            ));
        }

        let index = build_key_index(&batches, key_column)?;

        Ok(Self {
            _buffer: buffer,
            batches,
            index,
        })
    }

    pub fn get(&self, keys: &[&str], columns: &[&str]) -> Result<RecordBatch, MurrError> {
        // 1. Look up keys and group by batch, preserving output order
        let mut batch_groups: Vec<Vec<(usize, u32)>> = vec![Vec::new(); self.batches.len()];

        for (output_pos, key) in keys.iter().enumerate() {
            if let Some(&(batch_idx, row_offset)) = self.index.get(*key) {
                batch_groups[batch_idx as usize].push((output_pos, row_offset));
            }
        }

        // 2. Get schema from first batch
        let schema = self.batches[0].schema();
        let fields: Result<Vec<_>, _> = columns
            .iter()
            .map(|name| {
                schema.field_with_name(name).cloned().map_err(|e| {
                    MurrError::TableError(format!("Column '{}' not found: {}", name, e))
                })
            })
            .collect();
        let result_schema = Arc::new(Schema::new(fields?));

        // 3. Gather from each batch and track output positions
        let mut all_results: Vec<(usize, RecordBatch)> = Vec::new();

        for (batch_idx, positions) in batch_groups.iter().enumerate() {
            if positions.is_empty() {
                continue;
            }

            let batch = &self.batches[batch_idx];
            let indices: UInt32Array = positions.iter().map(|(_, offset)| *offset).collect();

            let arrays: Result<Vec<_>, _> = columns
                .iter()
                .map(|name| {
                    let col_index = schema.index_of(name)?;
                    let col = batch.column(col_index);
                    take(col, &indices, None)
                })
                .collect();

            let partial = RecordBatch::try_new(result_schema.clone(), arrays?)?;

            for (i, (output_pos, _)) in positions.iter().enumerate() {
                all_results.push((*output_pos, partial.slice(i, 1)));
            }
        }

        // 4. Sort by output position and concatenate
        all_results.sort_by_key(|(pos, _)| *pos);

        if all_results.is_empty() {
            let empty_arrays: Vec<_> = columns
                .iter()
                .map(|name| {
                    let col_index = schema.index_of(name).unwrap();
                    self.batches[0].column(col_index).slice(0, 0)
                })
                .collect();
            return Ok(RecordBatch::try_new(result_schema, empty_arrays)?);
        }

        let batches_to_concat: Vec<_> = all_results.iter().map(|(_, b)| b).collect();
        let result = arrow::compute::concat_batches(&result_schema, batches_to_concat)?;
        Ok(result)
    }
}

fn build_key_index(batches: &[RecordBatch], key_column: &str) -> Result<KeyIndex, MurrError> {
    let mut index = HashMap::new();
    for (batch_idx, batch) in batches.iter().enumerate() {
        let schema = batch.schema();
        let col_index = schema.index_of(key_column).map_err(|e| {
            MurrError::TableError(format!("Key column {} not found: {}", key_column, e))
        })?;
        let col = batch.column(col_index);
        let string_array = col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| MurrError::TableError("Key column must be Utf8 type".to_string()))?;
        for i in 0..string_array.len() {
            if !string_array.is_null(i) {
                index.insert(
                    string_array.value(i).to_string(),
                    (batch_idx as u32, i as u32),
                );
            }
        }
    }

    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float32Array;
    use arrow::datatypes::{DataType, Field};
    use arrow::ipc::writer::FileWriter;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_arrow_file(num_batches: usize, rows_per_batch: usize) -> NamedTempFile {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Float32, false),
        ]));

        let mut file = NamedTempFile::new().unwrap();
        {
            let mut writer = FileWriter::try_new(&mut file, &schema).unwrap();

            for batch_idx in 0..num_batches {
                let start = batch_idx * rows_per_batch;
                let keys: StringArray = (start..start + rows_per_batch)
                    .map(|i| Some(i.to_string()))
                    .collect();
                let values: Float32Array = (start..start + rows_per_batch)
                    .map(|i| Some(i as f32))
                    .collect();

                let batch =
                    RecordBatch::try_new(schema.clone(), vec![Arc::new(keys), Arc::new(values)])
                        .unwrap();

                writer.write(&batch).unwrap();
            }
            writer.finish().unwrap();
        }
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_table_get_single_batch() {
        let file = create_test_arrow_file(1, 1000);
        let table = Table::open(file.path(), "key").unwrap();

        let result = table.get(&["10", "500", "999"], &["value"]).unwrap();
        assert_eq!(result.num_rows(), 3);

        let values = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();

        assert_eq!(values.value(0), 10.0);
        assert_eq!(values.value(1), 500.0);
        assert_eq!(values.value(2), 999.0);
    }

    #[test]
    fn test_table_get_multi_batch() {
        let file = create_test_arrow_file(3, 100); // 3 batches, 100 rows each (keys 0-299)
        let table = Table::open(file.path(), "key").unwrap();

        // Query keys from different batches
        let result = table.get(&["50", "150", "250"], &["value"]).unwrap();
        assert_eq!(result.num_rows(), 3);

        let values = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();

        // key "50" -> batch 0, value 50.0
        // key "150" -> batch 1, value 150.0
        // key "250" -> batch 2, value 250.0
        assert_eq!(values.value(0), 50.0);
        assert_eq!(values.value(1), 150.0);
        assert_eq!(values.value(2), 250.0);
    }
}
