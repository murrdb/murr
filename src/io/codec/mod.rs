pub mod bool_;
pub mod float32;
pub mod float64;
pub mod int16;
pub mod int32;
pub mod int64;
pub mod int8;
pub mod primitive;
pub mod uint16;
pub mod uint32;
pub mod uint64;
pub mod uint8;
pub mod utf8;

use arrow::{
    array::{Array, ArrayRef},
    datatypes::DataType,
};
use serde_json::Value;

use crate::{
    core::{DType, MurrError},
    io::{
        row::{read::ReadRow, write::WriteRow},
        schema::SegmentColumnSchema,
    },
};

pub trait Codec: Send + Sync {
    fn dtype(&self) -> DType;
    fn arrow_dtype(&self) -> DataType;

    fn to_json(&self, arr: &dyn Array) -> Result<Vec<Value>, MurrError>;
    fn from_json(&self, vals: &[Value]) -> Result<ArrayRef, MurrError>;

    fn make_encoder(
        &self,
        col: SegmentColumnSchema,
        rows: usize,
    ) -> Box<dyn ColumnEncoder>;
    fn make_decoder(
        &self,
        col: SegmentColumnSchema,
        arr: &dyn Array,
    ) -> Result<Box<dyn ColumnDecoder>, MurrError>;
}

pub trait ColumnEncoder: Send {
    fn add_row(&mut self, row: &ReadRow) -> Result<(), MurrError>;
    fn add_empty(&mut self) -> Result<(), MurrError>;
    fn build(&mut self) -> ArrayRef;
}

pub trait ColumnDecoder: Send + Sync {
    fn write_to_row(&self, index: usize, row: &mut WriteRow);
}

pub fn codec_for(dtype: DType) -> &'static dyn Codec {
    match dtype {
        DType::Utf8 => &utf8::Utf8Codec,
        DType::Bool => &bool_::BoolCodec,
        DType::Int8 => &int8::Int8Codec,
        DType::Int16 => &int16::Int16Codec,
        DType::Int32 => &int32::Int32Codec,
        DType::Int64 => &int64::Int64Codec,
        DType::UInt8 => &uint8::UInt8Codec,
        DType::UInt16 => &uint16::UInt16Codec,
        DType::UInt32 => &uint32::UInt32Codec,
        DType::UInt64 => &uint64::UInt64Codec,
        DType::Float32 => &float32::Float32Codec,
        DType::Float64 => &float64::Float64Codec,
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

    use arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int32Array, StringArray, UInt64Array,
    };

    use super::*;
    use crate::io::schema::SegmentSchema;

    fn col(index: u32, dtype: DType, name: &str, offset: u32) -> SegmentColumnSchema {
        SegmentColumnSchema {
            index,
            dtype,
            name: name.into(),
            offset,
        }
    }

    #[test]
    fn factory_roundtrip_mixed_dtypes() {
        let cols = vec![
            col(0, DType::Float32, "f32", 0),
            col(1, DType::Float64, "f64", 4),
            col(2, DType::Int32, "i32", 12),
            col(3, DType::UInt64, "u64", 16),
            col(4, DType::Bool, "b", 24),
            col(5, DType::Utf8, "s", 25),
        ];
        let schema = SegmentSchema::new(&cols);

        let f32_in =
            Float32Array::from(vec![Some(1.5), None, Some(-2.5), Some(0.0), Some(f32::NAN)]);
        let f64_in = Float64Array::from(vec![Some(1.0), Some(-1e10), None, Some(0.0), Some(2.5)]);
        let i32_in = Int32Array::from(vec![Some(-7), Some(0), None, Some(i32::MAX), Some(42)]);
        let u64_in = UInt64Array::from(vec![
            Some(0),
            Some(1u64 << 60),
            Some(u64::MAX),
            None,
            Some(1234567890123456789),
        ]);
        let b_in =
            BooleanArray::from(vec![Some(true), Some(false), None, Some(true), Some(false)]);
        let s_in = StringArray::from(vec![Some("hi"), Some(""), Some("δ"), None, Some("world")]);
        let n = f32_in.len();

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(f32_in.clone()),
            Arc::new(f64_in.clone()),
            Arc::new(i32_in.clone()),
            Arc::new(u64_in.clone()),
            Arc::new(b_in.clone()),
            Arc::new(s_in.clone()),
        ];

        let decoders: Vec<Box<dyn ColumnDecoder>> = cols
            .iter()
            .zip(arrays.iter())
            .map(|(c, a)| codec_for(c.dtype).make_decoder(c.clone(), a.as_ref()).unwrap())
            .collect();

        let row_buffers: Vec<Vec<u8>> = (0..n)
            .map(|i| {
                let mut wrow = WriteRow::new(&schema, "");
                for d in &decoders {
                    d.write_to_row(i, &mut wrow);
                }
                wrow.bytes
            })
            .collect();

        let mut encoders: Vec<Box<dyn ColumnEncoder>> = cols
            .iter()
            .map(|c| codec_for(c.dtype).make_encoder(c.clone(), n))
            .collect();
        for buf in &row_buffers {
            let row = ReadRow::new(&schema, buf);
            for e in &mut encoders {
                e.add_row(&row).unwrap();
            }
        }
        let out: Vec<ArrayRef> = encoders.iter_mut().map(|e| e.build()).collect();

        let f32_out = out[0].as_any().downcast_ref::<Float32Array>().unwrap();
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
            out[2].as_any().downcast_ref::<Int32Array>().unwrap(),
            &i32_in
        );
        assert_eq!(
            out[3].as_any().downcast_ref::<UInt64Array>().unwrap(),
            &u64_in
        );
        assert_eq!(
            out[4].as_any().downcast_ref::<BooleanArray>().unwrap(),
            &b_in
        );
        assert_eq!(
            out[5].as_any().downcast_ref::<StringArray>().unwrap(),
            &s_in
        );
    }

    #[test]
    fn make_decoder_rejects_dtype_mismatch() {
        let c = col(0, DType::Float32, "x", 0);
        let wrong: ArrayRef = Arc::new(StringArray::from(vec!["nope"]));
        let err = codec_for(c.dtype).make_decoder(c.clone(), wrong.as_ref());
        assert!(matches!(err, Err(MurrError::SegmentError(_))));
    }

    #[test]
    fn json_roundtrip_across_dtypes() {
        let cases: Vec<(DType, ArrayRef)> = vec![
            (
                DType::Int32,
                Arc::new(Int32Array::from(vec![Some(-7), None, Some(42)])),
            ),
            (
                DType::UInt64,
                Arc::new(UInt64Array::from(vec![Some((1u64 << 53) + 1), None])),
            ),
            (
                DType::Bool,
                Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            ),
            (
                DType::Utf8,
                Arc::new(StringArray::from(vec![Some("hi"), None, Some("")])),
            ),
        ];
        for (dt, arr) in cases {
            let json = codec_for(dt).to_json(arr.as_ref()).unwrap();
            let back = codec_for(dt).from_json(&json).unwrap();
            assert_eq!(arr.as_ref().to_data(), back.as_ref().to_data(), "{dt:?}");
        }
    }
}
