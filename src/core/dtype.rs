use arrow::datatypes::DataType;

use crate::core::DTypeName;

pub trait DType: Send + Sync + 'static {
    fn name(&self) -> DTypeName;
    fn arrow_dtype(&self) -> DataType;
    fn size(&self) -> usize;
}
