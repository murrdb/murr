use arrow::array::{Array, ArrowPrimitiveType, PrimitiveArray};

use crate::{
    core::MurrError,
    io3::{
        batch::RowBatch, column::{ArrayDecoder, ArrayEncoder}, model::SegmentColumnSchema, row::Row
    },
};

pub trait PrimitiveArrayEncoder {
    type ArrowType: ArrowPrimitiveType;

    fn set_primitive(
        row: &mut Row,
        bitset_size: usize,
        offset: usize,
        value: &<Self::ArrowType as ArrowPrimitiveType>::Native,
    );
}

impl<T: PrimitiveArrayEncoder> ArrayEncoder for T {
    fn encode_to(
        column: &SegmentColumnSchema,
        array: &dyn Array,
        rows: &mut RowBatch,
    ) -> Result<(), MurrError> {
        let data = array
            .as_any()
            .downcast_ref::<PrimitiveArray<T::ArrowType>>()
            .ok_or_else(|| {
                MurrError::SegmentError(format!(
                    "expected {:?}, got {:?}",
                    T::ArrowType::DATA_TYPE,
                    array.data_type()
                ))
            })?;

        let bitset_size = rows.schema.bitset_size;
        for (index, value) in data.iter().enumerate() {
            let row = &mut rows.rows[index];
            match value {
                None => row.set_null(column.index as usize),
                Some(v) => T::set_primitive(row, bitset_size as usize, column.offset as usize, &v),
            }
        }
        Ok(())
    }
}

pub trait PrimitiveArrayDecoder {
    type ArrowType: ArrowPrimitiveType;

    fn get_primitive(
        row: &Row,
        bitset_size: usize,
        offset: usize,
    ) -> <Self::ArrowType as ArrowPrimitiveType>::Native;
}

impl<T: PrimitiveArrayDecoder> ArrayDecoder for T {
    type A = PrimitiveArray<T::ArrowType>;

    fn decode_to(column: &SegmentColumnSchema, rows: &RowBatch) -> Result<Self::A, MurrError> {
        let bitset_size = rows.schema.bitset_size as usize;
        let offset = column.offset as usize;
        let col_index = column.index as usize;
        let array = rows
            .rows
            .iter()
            .map(|row| {
                if row.is_null(col_index) {
                    None
                } else {
                    Some(T::get_primitive(row, bitset_size, offset))
                }
            })
            .collect::<PrimitiveArray<T::ArrowType>>();
        Ok(array)
    }
}
