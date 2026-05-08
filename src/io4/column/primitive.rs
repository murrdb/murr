use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, PrimitiveArray, PrimitiveBuilder};
use bytemuck::{NoUninit, Pod};

use crate::{
    core::MurrError,
    io4::{
        column::{ColumnDecoder, ColumnEncoder, downcast},
        row::{read::ReadRow, write::WriteRow},
        schema::SegmentColumnSchema,
    },
};

pub struct PrimitiveEncoder<T: ArrowPrimitiveType>
where
    T::Native: Pod,
{
    column: SegmentColumnSchema,
    builder: PrimitiveBuilder<T>,
}

impl<T> PrimitiveEncoder<T>
where
    T: ArrowPrimitiveType,
    T::Native: Pod,
{
    pub fn new(column: SegmentColumnSchema, rows: usize) -> Self {
        Self {
            column,
            builder: PrimitiveBuilder::<T>::with_capacity(rows),
        }
    }
}

impl<T> ColumnEncoder for PrimitiveEncoder<T>
where
    T: ArrowPrimitiveType + 'static,
    T::Native: Pod,
{
    fn add_row(&mut self, row: &ReadRow) -> Result<(), MurrError> {
        if row.is_null(&self.column) {
            self.builder.append_null();
        } else {
            self.builder
                .append_value(row.read_static::<T::Native>(&self.column));
        }
        Ok(())
    }

    fn add_empty(&mut self) -> Result<(), MurrError> {
        self.builder.append_null();
        Ok(())
    }

    fn build(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

pub struct PrimitiveDecoder<T: ArrowPrimitiveType>
where
    T::Native: NoUninit,
{
    column: SegmentColumnSchema,
    array: PrimitiveArray<T>,
}

impl<T> PrimitiveDecoder<T>
where
    T: ArrowPrimitiveType + 'static,
    T::Native: NoUninit,
{
    pub fn new(column: SegmentColumnSchema, array: &dyn Array) -> Result<Self, MurrError> {
        let typed = downcast::<PrimitiveArray<T>>(array, &format!("{:?}", T::DATA_TYPE))?;
        Ok(Self {
            column,
            array: typed.clone(),
        })
    }
}

impl<T> ColumnDecoder for PrimitiveDecoder<T>
where
    T: ArrowPrimitiveType + 'static,
    T::Native: NoUninit,
{
    fn write_to_row(&self, index: usize, row: &mut WriteRow) -> Result<(), MurrError> {
        if !self.array.is_null(index) {
            row.write_static(&self.column, self.array.value(index));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Float32Array, Float64Array, StringArray};
    use arrow::datatypes::{Float32Type, Float64Type};

    use super::*;
    use crate::{
        core::DType,
        io4::{row::read::ReadRow, schema::SegmentSchema},
    };

    fn single(dtype: DType) -> (SegmentSchema, SegmentColumnSchema) {
        let c = SegmentColumnSchema {
            index: 0,
            dtype,
            name: "v".into(),
            offset: 0,
        };
        (SegmentSchema::new(std::slice::from_ref(&c)), c)
    }

    #[test]
    fn f32_roundtrip_with_nulls_and_nan() {
        let (schema, c) = single(DType::Float32);
        let input = Float32Array::from(vec![
            Some(1.5_f32),
            None,
            Some(-2.5),
            Some(0.0),
            Some(f32::NAN),
        ]);

        let dec = PrimitiveDecoder::<Float32Type>::new(c.clone(), &input).unwrap();
        let bufs: Vec<Vec<u8>> = (0..input.len())
            .map(|i| {
                let mut w = WriteRow::new(&schema, "");
                dec.write_to_row(i, &mut w).unwrap();
                w.bytes
            })
            .collect();

        let mut enc = PrimitiveEncoder::<Float32Type>::new(c, input.len());
        for b in &bufs {
            enc.add_row(&ReadRow::new(&schema, b)).unwrap();
        }
        let out_arr = enc.build();
        let out = out_arr.as_any().downcast_ref::<Float32Array>().unwrap();

        assert_eq!(out.len(), input.len());
        for i in 0..input.len() {
            assert_eq!(out.is_null(i), input.is_null(i));
            if !input.is_null(i) {
                let (v, vb) = (input.value(i), out.value(i));
                if v.is_nan() {
                    assert!(vb.is_nan());
                } else {
                    assert_eq!(v, vb);
                }
            }
        }
    }

    #[test]
    fn f64_roundtrip() {
        let (schema, c) = single(DType::Float64);
        let input = Float64Array::from(vec![Some(1.0), Some(-1e10), None, Some(0.0)]);

        let dec = PrimitiveDecoder::<Float64Type>::new(c.clone(), &input).unwrap();
        let bufs: Vec<Vec<u8>> = (0..input.len())
            .map(|i| {
                let mut w = WriteRow::new(&schema, "");
                dec.write_to_row(i, &mut w).unwrap();
                w.bytes
            })
            .collect();

        let mut enc = PrimitiveEncoder::<Float64Type>::new(c, input.len());
        for b in &bufs {
            enc.add_row(&ReadRow::new(&schema, b)).unwrap();
        }
        let out_arr = enc.build();
        assert_eq!(
            out_arr.as_any().downcast_ref::<Float64Array>().unwrap(),
            &input
        );
    }

    #[test]
    fn decoder_rejects_wrong_array_type() {
        let (_schema, c) = single(DType::Float32);
        let wrong = StringArray::from(vec!["not a float"]);
        let err = PrimitiveDecoder::<Float32Type>::new(c, &wrong);
        assert!(matches!(err, Err(MurrError::SegmentError(_))));
    }
}
