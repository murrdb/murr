use std::marker::PhantomData;

use arrow::array::{ArrayRef, ArrowPrimitiveType, PrimitiveArray};
use bytemuck::Pod;

use crate::{
    core::MurrError,
    io::{
        column::{ColumnCodec, downcast},
        model::SegmentColumnSchema,
        row::Row,
    },
};

pub struct PrimitiveCodec<T: ArrowPrimitiveType>(pub PhantomData<fn() -> T>);

impl<T> ColumnCodec for PrimitiveCodec<T>
where
    T: ArrowPrimitiveType + 'static,
    T::Native: Pod,
{
    fn encode(
        &self,
        col: &SegmentColumnSchema,
        bitset_size: usize,
        array: &dyn arrow::array::Array,
        rows: &mut [Row],
    ) -> Result<(), MurrError> {
        let data = downcast::<PrimitiveArray<T>>(array, &format!("{:?}", T::DATA_TYPE))?;
        for (index, value) in data.iter().enumerate() {
            let row = &mut rows[index];
            match value {
                None => row.set_null(col.index as usize),
                Some(v) => row.write_static(bitset_size, col.offset as usize, v),
            }
        }
        Ok(())
    }

    fn decode(
        &self,
        col: &SegmentColumnSchema,
        bitset_size: usize,
        rows: &[Row],
    ) -> Result<ArrayRef, MurrError> {
        let offset = col.offset as usize;
        let col_index = col.index as usize;
        let array: PrimitiveArray<T> = rows
            .iter()
            .map(|row| {
                if row.is_null(col_index) {
                    None
                } else {
                    Some(row.read_static::<T::Native>(bitset_size, offset))
                }
            })
            .collect();
        Ok(std::sync::Arc::new(array))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Float32Array, Float64Array, StringArray};
    use arrow::datatypes::{Float32Type, Float64Type};

    use super::*;
    use crate::{core::DType, io::model::SegmentSchema};

    fn single_col_schema(dtype: DType) -> SegmentSchema {
        SegmentSchema::new(&vec![SegmentColumnSchema {
            index: 0,
            dtype,
            name: "v".into(),
            offset: 0,
        }])
    }

    fn fresh_rows(schema: &SegmentSchema, n: usize) -> Vec<Row> {
        (0..n)
            .map(|_| Row::new(schema, schema.bitset_size, schema.capacity))
            .collect()
    }

    #[test]
    fn f32_roundtrip_with_nulls_and_nan() {
        let schema = single_col_schema(DType::Float32);
        let codec = PrimitiveCodec::<Float32Type>(PhantomData);
        let array = Float32Array::from(vec![
            Some(1.5_f32),
            None,
            Some(-2.5),
            Some(0.0),
            Some(f32::NAN),
        ]);
        let mut rows = fresh_rows(&schema, array.len());
        codec
            .encode(&schema.columns[0], schema.bitset_size, &array, &mut rows)
            .unwrap();
        let decoded = codec
            .decode(&schema.columns[0], schema.bitset_size, &rows)
            .unwrap();
        let back = decoded.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(back.len(), array.len());
        for i in 0..array.len() {
            assert_eq!(back.is_null(i), array.is_null(i), "row {i}");
            if !array.is_null(i) {
                let v = array.value(i);
                let v_back = back.value(i);
                if v.is_nan() {
                    assert!(v_back.is_nan(), "row {i}: expected NaN");
                } else {
                    assert_eq!(v, v_back, "row {i}");
                }
            }
        }
    }

    #[test]
    fn f64_roundtrip() {
        let schema = single_col_schema(DType::Float64);
        let codec = PrimitiveCodec::<Float64Type>(PhantomData);
        let array = Float64Array::from(vec![Some(3.14159), Some(-1e10), None, Some(0.0)]);
        let mut rows = fresh_rows(&schema, array.len());
        codec
            .encode(&schema.columns[0], schema.bitset_size, &array, &mut rows)
            .unwrap();
        let decoded = codec
            .decode(&schema.columns[0], schema.bitset_size, &rows)
            .unwrap();
        let back = decoded.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(back, &array);
    }

    #[test]
    fn encode_rejects_wrong_array_type() {
        let schema = single_col_schema(DType::Float32);
        let codec = PrimitiveCodec::<Float32Type>(PhantomData);
        let wrong = StringArray::from(vec!["not a float"]);
        let mut rows = fresh_rows(&schema, 1);
        let err = codec.encode(&schema.columns[0], schema.bitset_size, &wrong, &mut rows);
        assert!(matches!(err, Err(MurrError::SegmentError(_))));
    }
}
