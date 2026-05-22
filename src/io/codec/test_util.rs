//! Test-only helpers for codec roundtrips. Used by per-dtype unit tests to
//! avoid duplicating identical row/json roundtrip bodies.
use arrow::array::Array;

use crate::core::DType;
use crate::io::codec::codec_for;
use crate::io::row::{read::ReadRow, write::WriteRow};
use crate::io::schema::{SegmentColumnSchema, SegmentSchema};

fn single_column_schema(dtype: DType) -> (SegmentSchema, SegmentColumnSchema) {
    let c = SegmentColumnSchema {
        index: 0,
        dtype,
        name: "v".into(),
        offset: 0,
    };
    (SegmentSchema::new(std::slice::from_ref(&c)), c)
}

/// Encode `input` to row buffers via the dtype's decoder, decode back via its
/// encoder, and assert the resulting Arrow array equals `input` bit-for-bit.
/// Inputs that round-trip non-bit-identically (NaN, lossy float casts) need a
/// dedicated dtype-specific test instead.
pub fn assert_row_roundtrip(dtype: DType, input: &dyn Array) {
    let (schema, c) = single_column_schema(dtype);
    let codec = codec_for(dtype);

    let dec = codec.make_decoder(c.clone(), input).unwrap();
    let bufs: Vec<Vec<u8>> = (0..input.len())
        .map(|i| {
            let mut w = WriteRow::new(&schema, "");
            dec.write_to_row(i, &mut w);
            w.bytes
        })
        .collect();

    let mut enc = codec.make_encoder(c, input.len());
    for b in &bufs {
        enc.add_row(&ReadRow::new(&schema, b)).unwrap();
    }
    let out = enc.build();
    assert_eq!(input.to_data(), out.to_data(), "row roundtrip for {dtype:?}");
}

/// Encode `input` to JSON via the dtype codec, decode back, and assert
/// bit-equal Arrow data.
pub fn assert_json_roundtrip(dtype: DType, input: &dyn Array) {
    let codec = codec_for(dtype);
    let json = codec.to_json(input).unwrap();
    let back = codec.from_json(&json).unwrap();
    assert_eq!(
        input.to_data(),
        back.to_data(),
        "json roundtrip for {dtype:?}"
    );
}
