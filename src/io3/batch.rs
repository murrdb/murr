use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::DataType,
};

use crate::{
    core::{DType, MurrError},
    io3::column::{ArrayDecoder, ArrayEncoder},
    io3::{
        model::{SegmentColumnSchema, SegmentSchema},
        row::Row,
    },
};
pub struct ColumnBatch {
    pub schema: SegmentSchema,
    pub columns: Vec<ArrayRef>,
    pub row_count: usize,
}

impl TryFrom<RecordBatch> for ColumnBatch {
    type Error = MurrError;
    fn try_from(batch: RecordBatch) -> Result<Self, Self::Error> {
        let fields = batch.schema().fields().clone();
        let mut columns = Vec::with_capacity(fields.len());
        let mut offset: u32 = 0;
        for (i, field) in fields.iter().enumerate() {
            let dtype = DType::try_from(field.data_type())?;
            columns.push(SegmentColumnSchema {
                index: i as u32,
                dtype,
                name: field.name().clone(),
                offset,
            });
            offset += dtype.size() as u32;
        }
        let schema = SegmentSchema::new(&columns);
        Ok(ColumnBatch {
            schema,
            columns: batch.columns().to_vec(),
            row_count: batch.num_rows(),
        })
    }
}

pub struct RowBatch {
    pub schema: SegmentSchema,
    pub rows: Vec<Row>,
}

impl RowBatch {
    fn new(schema: &SegmentSchema, rows: usize) -> Self {
        let bitset_size = schema.bitset_size();
        let capacity = schema.capacity();
        RowBatch {
            schema: schema.clone(),
            rows: (0..rows)
                .map(|_| Row::new(schema, bitset_size, capacity))
                .collect(),
        }
    }
}

impl TryFrom<RowBatch> for ColumnBatch {
    type Error = MurrError;
    fn try_from(value: RowBatch) -> Result<Self, Self::Error> {
        let row_count = value.rows.len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(value.schema.columns.len());
        for column in &value.schema.columns {
            let array: ArrayRef = match column.dtype {
                DType::Float32 => Arc::new(f32::decode_to(column, &value)?),
                DType::Float64 => Arc::new(f64::decode_to(column, &value)?),
                DType::Utf8 => Arc::new(String::decode_to(column, &value)?),
            };
            columns.push(array);
        }
        Ok(ColumnBatch {
            schema: value.schema,
            columns,
            row_count,
        })
    }
}

impl TryFrom<ColumnBatch> for RowBatch {
    type Error = MurrError;

    fn try_from(batch: ColumnBatch) -> Result<Self, Self::Error> {
        let mut row_batch = RowBatch::new(&batch.schema, batch.row_count);
        for (column, array) in batch.schema.columns.iter().zip(batch.columns.iter()) {
            match array.data_type() {
                DataType::Float32 => f32::encode_to(column, array.as_ref(), &mut row_batch)?,
                DataType::Float64 => f64::encode_to(column, array.as_ref(), &mut row_batch)?,
                DataType::Utf8 => String::encode_to(column, array.as_ref(), &mut row_batch)?,
                dt => {
                    return Err(MurrError::SegmentError(format!("unsupported dtype {dt:?}")));
                }
            }
        }
        Ok(row_batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float32Array, Float64Array, StringArray};
    use arrow::datatypes::{Field, Schema};

    fn make_mixed_batch() -> (RecordBatch, StringArray, Float32Array, Float64Array) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float32, true),
            Field::new("weight", DataType::Float64, true),
        ]));
        let name = StringArray::from(vec![
            Some("alpha"),
            None,
            Some(""),
            Some("gamma"),
            Some("δ-unicode"),
        ]);
        let score = Float32Array::from(vec![Some(1.5), Some(-2.25), None, Some(0.0), Some(42.5)]);
        let weight = Float64Array::from(vec![
            None,
            Some(3.14159),
            Some(-1e10),
            Some(0.0),
            Some(f64::NAN),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(name.clone()),
                Arc::new(score.clone()),
                Arc::new(weight.clone()),
            ],
        )
        .unwrap();
        (batch, name, score, weight)
    }

    #[test]
    fn record_batch_to_column_batch() {
        let (batch, name, score, weight) = make_mixed_batch();
        let cb: ColumnBatch = batch.try_into().unwrap();

        assert_eq!(cb.row_count, 5);
        assert_eq!(
            cb.columns[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap(),
            &name
        );
        assert_eq!(
            cb.columns[1]
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap(),
            &score
        );
        assert_eq!(
            cb.columns[2]
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap(),
            &weight
        );
    }

    #[test]
    fn column_batch_row_batch_round_trip() {
        let (batch, name, score, weight) = make_mixed_batch();
        let cb: ColumnBatch = batch.try_into().unwrap();
        let rb: RowBatch = cb.try_into().unwrap();
        let cb2: ColumnBatch = rb.try_into().unwrap();

        assert_eq!(cb2.row_count, 5);

        let name_back = cb2.columns[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_back, &name);

        let score_back = cb2.columns[1]
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(score_back, &score);

        let weight_back = cb2.columns[2]
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(weight_back.len(), weight.len());
        for i in 0..weight.len() {
            assert_eq!(weight_back.is_null(i), weight.is_null(i), "row {i}");
            if !weight.is_null(i) {
                let v = weight.value(i);
                let v_back = weight_back.value(i);
                if v.is_nan() {
                    assert!(v_back.is_nan(), "row {i}: expected NaN, got {v_back}");
                } else {
                    assert_eq!(v_back, v, "row {i}");
                }
            }
        }
    }
}
