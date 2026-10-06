use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, BinaryArray, BinaryBuilder, PrimitiveArray,
    PrimitiveBuilder,
};
use bytemuck::{NoUninit, Pod};
use integer_encoding::VarInt;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{
    core::MurrError,
    io::{
        codec::{ColumnDecoder, ColumnEncoder, downcast},
        row::{read::ReadRow, write::WriteRow},
        schema::SegmentColumnSchema,
    },
};

// A 64-bit value takes at most 10 LEB128 bytes.
const MAX_VARINT_SIZE: usize = 10;

pub struct Encoder<T: ArrowPrimitiveType>
where
    T::Native: Pod,
{
    column: SegmentColumnSchema,
    builder: PrimitiveBuilder<T>,
}

impl<T> Encoder<T>
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

impl<T> ColumnEncoder for Encoder<T>
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

    fn build(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

pub struct Decoder<T: ArrowPrimitiveType>
where
    T::Native: NoUninit,
{
    column: SegmentColumnSchema,
    array: PrimitiveArray<T>,
}

impl<T> Decoder<T>
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

impl<T> ColumnDecoder for Decoder<T>
where
    T: ArrowPrimitiveType + 'static,
    T::Native: NoUninit,
{
    fn write_to_row(&self, index: usize, row: &mut WriteRow) {
        if !self.array.is_null(index) {
            row.write_static(&self.column, self.array.value(index));
        }
    }
}

pub fn encode_keys<T>(arr: &dyn Array) -> Result<BinaryArray, MurrError>
where
    T: ArrowPrimitiveType + 'static,
    T::Native: VarInt,
{
    let typed = downcast::<PrimitiveArray<T>>(arr, &format!("{:?}", T::DATA_TYPE))?;
    let mut builder = BinaryBuilder::with_capacity(typed.len(), typed.len() * MAX_VARINT_SIZE);
    let mut buf = [0u8; MAX_VARINT_SIZE];
    for value in typed.values() {
        let size = value.encode_var(&mut buf);
        builder.append_value(&buf[..size]);
    }
    Ok(builder.finish())
}

pub fn to_json<T>(arr: &dyn Array) -> Result<Vec<Value>, MurrError>
where
    T: ArrowPrimitiveType + 'static,
    T::Native: Serialize,
{
    let typed = downcast::<PrimitiveArray<T>>(arr, &format!("{:?}", T::DATA_TYPE))?;
    Ok((0..typed.len())
        .map(|i| {
            if typed.is_null(i) {
                Value::Null
            } else {
                serde_json::to_value(typed.value(i)).expect("native scalar serializes to JSON")
            }
        })
        .collect())
}

pub fn from_json<T>(vals: &[Value]) -> Result<ArrayRef, MurrError>
where
    T: ArrowPrimitiveType + 'static,
    T::Native: DeserializeOwned,
{
    let arr: PrimitiveArray<T> = vals
        .iter()
        .map(|v| match v {
            Value::Null => Ok(None),
            other => serde_json::from_value::<T::Native>(other.clone())
                .map(Some)
                .map_err(|e| MurrError::TableError(e.to_string())),
        })
        .collect::<Result<_, _>>()?;
    Ok(Arc::new(arr))
}
