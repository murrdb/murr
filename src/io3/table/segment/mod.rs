use arrow::array::{RecordBatch, StringArray};

use crate::{
    core::{MurrError, TableSchema},
    io3::{
        batch::{ColumnBatch, RowBatch},
        model::OffsetSize,
        table::{index::keys::SegmentKeyBytes, segment::footer::SegmentFooterV1},
    },
};

pub mod footer;
pub mod trailer;

pub struct Segment {
    footer: SegmentFooterV1,
}

pub struct SegmentBytes {
    pub rows: Vec<u8>,
    pub keys: SegmentKeyBytes,
    pub footer: SegmentFooterV1,
}

impl SegmentBytes {
    pub fn to_bytes(&self) -> Result<Vec<u8>, MurrError> {
        let mut buf = Vec::with_capacity(self.keys.len() + self.rows.len() + 4096);
        buf.extend_from_slice(&self.rows);
        buf.extend_from_slice(&self.keys.bytes);
        buf.extend_from_slice(&self.footer.to_bytes()?);
        return Ok(buf);
    }
}

impl Segment {
    pub fn load(last_block: &[u8]) -> Result<Self, MurrError> {
        let footer = SegmentFooterV1::from_last_block(last_block)?;
        Ok(Segment { footer })
    }

    pub fn write(batch: RecordBatch, schema: &TableSchema) -> Result<SegmentBytes, MurrError> {
        let key_col_idx = batch.schema().index_of(&schema.key).map_err(|_| {
            MurrError::SegmentError(format!("key column '{}' not in record batch", schema.key))
        })?;
        let key_array = batch
            .column(key_col_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                MurrError::SegmentError(format!("key column '{}' is not Utf8", schema.key))
            })?
            .clone();

        let row_count = batch.num_rows() as u32;

        let column_batch: ColumnBatch = batch.try_into()?;
        let segment_schema = column_batch.schema.clone();
        let row_batch: RowBatch = column_batch.try_into()?;

        let mut rows_buf: Vec<u8> = Vec::with_capacity(row_count as usize * 16);
        let mut keys_buf = SegmentKeyBytes::with_capacity(row_count as usize * 16);

        let mut current_row_offset: u32 = 0;
        for (key_opt, row) in key_array.iter().zip(row_batch.rows.iter()) {
            let key = key_opt.ok_or_else(|| {
                MurrError::SegmentError(format!("key column '{}' has a null value", schema.key))
            })?;
            let row_size = row.bytes.len() as u32;

            rows_buf.extend_from_slice(&row_size.to_le_bytes());
            rows_buf.extend_from_slice(&row.bytes);

            keys_buf.write_key(key, current_row_offset, row_size);
            current_row_offset = current_row_offset
                .checked_add(4 + row_size)
                .ok_or_else(|| MurrError::SegmentError("row offset overflow u32".into()))?;
        }

        let rows_size = rows_buf.len() as u32;
        let keys_size = keys_buf.len() as u32;
        let footer = SegmentFooterV1 {
            row_count,
            schema: segment_schema,
            rows: OffsetSize {
                offset: 0,
                size: rows_size,
            },
            keys: OffsetSize {
                offset: rows_size,
                size: keys_size,
            },
        };

        Ok(SegmentBytes {
            rows: rows_buf,
            keys: keys_buf,
            footer,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ColumnSchema, DType};
    use crate::io3::table::segment::trailer::SegmentTrailer;
    use arrow::array::{Float32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn write_then_load_roundtrip() {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("score", DataType::Float32, true),
        ]));
        let ids = StringArray::from(vec!["k0", "k1", "k2"]);
        let scores = Float32Array::from(vec![Some(1.0), None, Some(-2.5)]);
        let batch =
            RecordBatch::try_new(arrow_schema, vec![Arc::new(ids), Arc::new(scores)]).unwrap();

        let mut columns = HashMap::new();
        columns.insert(
            "id".into(),
            ColumnSchema {
                dtype: DType::Utf8,
                nullable: false,
            },
        );
        columns.insert(
            "score".into(),
            ColumnSchema {
                dtype: DType::Float32,
                nullable: true,
            },
        );
        let table_schema = TableSchema {
            key: "id".into(),
            columns,
        };

        let segment_bytes = Segment::write(batch, &table_schema).unwrap();
        let bytes = segment_bytes.to_bytes().unwrap();
        let segment = Segment::load(&bytes).unwrap();
        let f = &segment.footer;

        assert_eq!(f.row_count, 3);
        assert_eq!(f.schema.columns.len(), 2);

        let trailer = SegmentTrailer::from_tail(&bytes).unwrap();
        assert_eq!(trailer.version, SegmentFooterV1::SEGMENT_FOOTER_VERSION);

        let by_name: HashMap<&str, DType> = f
            .schema
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c.dtype))
            .collect();
        assert_eq!(by_name.get("id"), Some(&DType::Utf8));
        assert_eq!(by_name.get("score"), Some(&DType::Float32));
    }
}
