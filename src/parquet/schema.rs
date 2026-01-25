use arrow::datatypes::{DataType, Schema};

use crate::conf::{ColumnConfig, DType, TableConfig};
use crate::core::MurrError;

/// Maps Murr's DType enum to Arrow DataType.
pub fn dtype_to_arrow(dtype: &DType) -> DataType {
    match dtype {
        DType::Utf8 => DataType::Utf8,
        DType::Int16 => DataType::Int16,
        DType::Int32 => DataType::Int32,
        DType::Int64 => DataType::Int64,
        DType::Uint16 => DataType::UInt16,
        DType::UInt32 => DataType::UInt32,
        DType::UInt64 => DataType::UInt64,
        DType::Float32 => DataType::Float32,
        DType::Float64 => DataType::Float64,
        DType::Bool => DataType::Boolean,
    }
}

/// Validates a Parquet file's Arrow schema against the TableConfig.
///
/// # Validation Rules (STRICT)
/// 1. All columns in TableConfig MUST exist in the Parquet schema
/// 2. Types MUST match exactly (no casting)
/// 3. Nullability: if config says nullable=false, Parquet column must be non-nullable
///
/// If `table_config.columns` is empty, validation is skipped (schema discovery mode).
pub fn validate_schema(
    parquet_schema: &Schema,
    table_config: &TableConfig,
) -> Result<(), MurrError> {
    if table_config.columns.is_empty() {
        return Ok(());
    }

    for (col_name, col_config) in &table_config.columns {
        validate_column(parquet_schema, col_name, col_config)?;
    }

    Ok(())
}

fn validate_column(
    parquet_schema: &Schema,
    col_name: &str,
    col_config: &ColumnConfig,
) -> Result<(), MurrError> {
    let field = parquet_schema.field_with_name(col_name).map_err(|_| {
        MurrError::ParquetError(format!(
            "Column '{}' defined in config not found in Parquet schema",
            col_name
        ))
    })?;

    let expected_type = dtype_to_arrow(&col_config.dtype);
    if field.data_type() != &expected_type {
        return Err(MurrError::ParquetError(format!(
            "Column '{}' type mismatch: config expects {:?}, Parquet has {:?}",
            col_name,
            expected_type,
            field.data_type()
        )));
    }

    if !col_config.nullable && field.is_nullable() {
        return Err(MurrError::ParquetError(format!(
            "Column '{}' nullability mismatch: config requires non-nullable, Parquet allows nulls",
            col_name
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use std::collections::HashMap;
    use std::time::Duration;

    use crate::conf::{LocalSourceConfig, SourceConfig};

    fn make_config(columns: Vec<(&str, DType, bool)>) -> TableConfig {
        TableConfig {
            source: SourceConfig::Local(LocalSourceConfig {
                path: "/tmp".to_string(),
            }),
            poll_interval: Duration::from_secs(60),
            parts: 1,
            key: vec!["id".to_string()],
            columns: columns
                .into_iter()
                .map(|(name, dtype, nullable)| (name.to_string(), ColumnConfig { dtype, nullable }))
                .collect(),
        }
    }

    #[test]
    fn test_dtype_to_arrow_mapping() {
        assert_eq!(dtype_to_arrow(&DType::Utf8), DataType::Utf8);
        assert_eq!(dtype_to_arrow(&DType::Int16), DataType::Int16);
        assert_eq!(dtype_to_arrow(&DType::Int32), DataType::Int32);
        assert_eq!(dtype_to_arrow(&DType::Int64), DataType::Int64);
        assert_eq!(dtype_to_arrow(&DType::Uint16), DataType::UInt16);
        assert_eq!(dtype_to_arrow(&DType::UInt32), DataType::UInt32);
        assert_eq!(dtype_to_arrow(&DType::UInt64), DataType::UInt64);
        assert_eq!(dtype_to_arrow(&DType::Float32), DataType::Float32);
        assert_eq!(dtype_to_arrow(&DType::Float64), DataType::Float64);
        assert_eq!(dtype_to_arrow(&DType::Bool), DataType::Boolean);
    }

    #[test]
    fn test_validate_schema_success() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Float32, true),
        ]);
        let config = make_config(vec![
            ("id", DType::Utf8, false),
            ("value", DType::Float32, true),
        ]);
        assert!(validate_schema(&schema, &config).is_ok());
    }

    #[test]
    fn test_validate_schema_missing_column() {
        let schema = Schema::new(vec![Field::new("id", DataType::Utf8, false)]);
        let config = make_config(vec![
            ("id", DType::Utf8, false),
            ("missing", DType::Float32, true),
        ]);
        let err = validate_schema(&schema, &config).unwrap_err();
        assert!(matches!(err, MurrError::ParquetError(_)));
        assert!(err.to_string().contains("missing"));
    }

    #[test]
    fn test_validate_schema_type_mismatch() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, true),
        ]);
        let config = make_config(vec![
            ("id", DType::Utf8, false),
            ("value", DType::Float32, true),
        ]);
        let err = validate_schema(&schema, &config).unwrap_err();
        assert!(matches!(err, MurrError::ParquetError(_)));
        assert!(err.to_string().contains("type mismatch"));
    }

    #[test]
    fn test_validate_schema_nullability_mismatch() {
        let schema = Schema::new(vec![Field::new("id", DataType::Utf8, true)]);
        let config = make_config(vec![("id", DType::Utf8, false)]);
        let err = validate_schema(&schema, &config).unwrap_err();
        assert!(matches!(err, MurrError::ParquetError(_)));
        assert!(err.to_string().contains("nullability"));
    }

    #[test]
    fn test_validate_schema_empty_config_skips_validation() {
        let schema = Schema::new(vec![Field::new("any", DataType::Utf8, true)]);
        let mut config = make_config(vec![]);
        config.columns = HashMap::new();
        assert!(validate_schema(&schema, &config).is_ok());
    }

    #[test]
    fn test_validate_schema_extra_parquet_columns_allowed() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("extra", DataType::Int64, true),
        ]);
        let config = make_config(vec![("id", DType::Utf8, false)]);
        assert!(validate_schema(&schema, &config).is_ok());
    }
}
