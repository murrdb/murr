use crate::io::column::{float32::Float32Codec, scalar::ScalarColumnWriter};

pub type Float32ColumnWriter = ScalarColumnWriter<Float32Codec>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DType;
    use crate::io::column::ColumnFooter;
    use crate::io::column::ColumnWriter;
    use crate::io::column::float32::footer::Float32ColumnFooter;
    use crate::io::info::ColumnInfo;
    use arrow::array::Float32Array;
    use bytemuck::cast_slice;
    use std::sync::Arc;

    fn non_nullable_info() -> Arc<ColumnInfo> {
        Arc::new(ColumnInfo {
            name: "score".to_string(),
            dtype: DType::Float32,
            nullable: false,
        })
    }

    fn nullable_info() -> Arc<ColumnInfo> {
        Arc::new(ColumnInfo {
            name: "score".to_string(),
            dtype: DType::Float32,
            nullable: true,
        })
    }

    fn make_array(values: &[Option<f32>]) -> Float32Array {
        values.iter().copied().collect::<Float32Array>()
    }

    fn make_non_null_array(values: &[f32]) -> Float32Array {
        Float32Array::from(values.to_vec())
    }

    #[test]
    fn write_non_nullable() {
        let writer = Float32ColumnWriter::new(non_nullable_info());

        let result = writer
            .write(&make_non_null_array(&[1.0, 2.5, 3.0]))
            .unwrap();
        assert_eq!(result.num_values, 3);

        let bytes = result.to_bytes();
        let footer = Float32ColumnFooter::parse(&bytes, 0).unwrap();
        assert_eq!(footer.payload.offset, 0);
        assert_eq!(footer.payload.size, 12);
        assert_eq!(footer.bitmap.size, 0);

        let payload: &[f32] = cast_slice(&bytes[0..12]);
        assert_eq!(payload, &[1.0, 2.5, 3.0]);
    }

    #[test]
    fn write_nullable_with_nulls() {
        let writer = Float32ColumnWriter::new(nullable_info());

        let result = writer
            .write(&make_array(&[Some(1.0), None, Some(3.0), None]))
            .unwrap();
        assert_eq!(result.num_values, 4);

        let bytes = result.to_bytes();
        let footer = Float32ColumnFooter::parse(&bytes, 0).unwrap();
        assert_eq!(footer.payload.size, 16);
        assert!(footer.bitmap.size > 0);

        let bitmap_start = footer.bitmap.offset as usize;
        let bitmap_end = bitmap_start + footer.bitmap.size as usize;
        let bitmap_words: &[u64] = cast_slice(&bytes[bitmap_start..bitmap_end]);
        // bit0=1, bit1=0, bit2=1, bit3=0 => 0b0101 = 5
        assert_eq!(bitmap_words[0], 0b0101);
    }

    #[test]
    fn write_nullable_no_nulls() {
        let writer = Float32ColumnWriter::new(nullable_info());

        let result = writer.write(&make_array(&[Some(1.0), Some(2.0)])).unwrap();

        let bytes = result.to_bytes();
        let footer = Float32ColumnFooter::parse(&bytes, 0).unwrap();
        assert_eq!(footer.bitmap.offset, 0);
        assert_eq!(footer.bitmap.size, 0);
    }

    #[test]
    fn write_empty() {
        let writer = Float32ColumnWriter::new(non_nullable_info());

        let result = writer.write(&make_non_null_array(&[])).unwrap();
        assert_eq!(result.num_values, 0);
    }
}
