pub mod primitive;
pub mod utf8;

use arrow::{
    array::{Array, ArrayRef},
    datatypes::{Float32Type, Float64Type},
};

use crate::{
    core::{DType, MurrError},
    io4::{
        column::{
            primitive::{PrimitiveDecoder, PrimitiveEncoder},
            utf8::{Utf8Decoder, Utf8Encoder},
        },
        row::{read::ReadRow, write::WriteRow},
        schema::SegmentColumnSchema,
    },
};

pub trait ColumnEncoder: Send {
    fn add_row(&mut self, row: &ReadRow) -> Result<(), MurrError>;
    fn add_empty(&mut self) -> Result<(), MurrError>;
    fn build(&mut self) -> ArrayRef;
}

pub trait ColumnDecoder: Send + Sync {
    fn write_to_row(&self, index: usize, row: &mut WriteRow) -> Result<(), MurrError>;
}

pub fn encoder_for(column: &SegmentColumnSchema, rows: usize) -> Box<dyn ColumnEncoder> {
    match column.dtype {
        DType::Float32 => Box::new(PrimitiveEncoder::<Float32Type>::new(column.clone(), rows)),
        DType::Float64 => Box::new(PrimitiveEncoder::<Float64Type>::new(column.clone(), rows)),
        DType::Utf8 => Box::new(Utf8Encoder::new(column.clone(), rows)),
    }
}

pub fn decoder_for(
    column: &SegmentColumnSchema,
    array: &dyn Array,
) -> Result<Box<dyn ColumnDecoder>, MurrError> {
    match column.dtype {
        DType::Float32 => Ok(Box::new(PrimitiveDecoder::<Float32Type>::new(
            column.clone(),
            array,
        )?)),
        DType::Float64 => Ok(Box::new(PrimitiveDecoder::<Float64Type>::new(
            column.clone(),
            array,
        )?)),
        DType::Utf8 => Ok(Box::new(Utf8Decoder::new(column.clone(), array)?)),
    }
}

pub(crate) fn downcast<'a, A: Array + 'static>(
    array: &'a dyn Array,
    expected: &str,
) -> Result<&'a A, MurrError> {
    array.as_any().downcast_ref::<A>().ok_or_else(|| {
        MurrError::SegmentError(format!("expected {expected}, got {:?}", array.data_type()))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float32Array, Float64Array, StringArray};

    use super::*;
    use crate::{
        core::DType,
        io4::{row::write::WriteRow, schema::SegmentSchema},
    };

    fn col(index: u32, dtype: DType, name: &str, offset: u32) -> SegmentColumnSchema {
        SegmentColumnSchema {
            index,
            dtype,
            name: name.into(),
            offset,
        }
    }

    #[test]
    fn factory_roundtrip_three_columns() {
        let cols = vec![
            col(0, DType::Float32, "f32", 0),
            col(1, DType::Float64, "f64", 4),
            col(2, DType::Utf8, "s", 12),
        ];
        let schema = SegmentSchema::new(&cols);

        let f32_in =
            Float32Array::from(vec![Some(1.5), None, Some(-2.5), Some(0.0), Some(f32::NAN)]);
        let f64_in = Float64Array::from(vec![Some(1.0), Some(-1e10), None, Some(0.0), Some(2.5)]);
        let s_in = StringArray::from(vec![Some("hi"), Some(""), Some("δ"), None, Some("world")]);
        let n = f32_in.len();

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(f32_in.clone()),
            Arc::new(f64_in.clone()),
            Arc::new(s_in.clone()),
        ];

        let decoders: Vec<Box<dyn ColumnDecoder>> = cols
            .iter()
            .zip(arrays.iter())
            .map(|(c, a)| decoder_for(c, a.as_ref()).unwrap())
            .collect();

        let row_buffers: Vec<Vec<u8>> = (0..n)
            .map(|i| {
                let mut wrow = WriteRow::new(&schema, "");
                for d in &decoders {
                    d.write_to_row(i, &mut wrow).unwrap();
                }
                wrow.bytes
            })
            .collect();

        let mut encoders: Vec<Box<dyn ColumnEncoder>> =
            cols.iter().map(|c| encoder_for(c, n)).collect();
        for buf in &row_buffers {
            let row = ReadRow::new(&schema, buf);
            for e in &mut encoders {
                e.add_row(&row).unwrap();
            }
        }
        let out: Vec<ArrayRef> = encoders.iter_mut().map(|e| e.build()).collect();

        let f32_out = out[0].as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(f32_out.len(), n);
        for i in 0..n {
            assert_eq!(f32_out.is_null(i), f32_in.is_null(i));
            if !f32_in.is_null(i) {
                let v = f32_in.value(i);
                let v_back = f32_out.value(i);
                if v.is_nan() {
                    assert!(v_back.is_nan());
                } else {
                    assert_eq!(v, v_back);
                }
            }
        }
        assert_eq!(
            out[1].as_any().downcast_ref::<Float64Array>().unwrap(),
            &f64_in
        );
        assert_eq!(
            out[2].as_any().downcast_ref::<StringArray>().unwrap(),
            &s_in
        );
    }

    #[test]
    fn decoder_for_rejects_dtype_mismatch() {
        let c = col(0, DType::Float32, "x", 0);
        let wrong: ArrayRef = Arc::new(StringArray::from(vec!["nope"]));
        let err = decoder_for(&c, wrong.as_ref());
        assert!(matches!(err, Err(MurrError::SegmentError(_))));
    }
}
