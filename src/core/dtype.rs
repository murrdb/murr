use arrow::datatypes::DataType;

use crate::core::DTypeName;

pub trait DType: Send + Sync + 'static {
    fn name(&self) -> DTypeName;
    fn arrow_dtype(&self) -> DataType;
    fn size(&self) -> usize;
    /// Arrow types whose every value has an exact form in this dtype.
    fn widens_from(&self, _from: &DataType) -> bool {
        false
    }
    /// Arrow types which fit this dtype only with rounding.
    fn rounds_from(&self, _from: &DataType) -> bool {
        false
    }
}
