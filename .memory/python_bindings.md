# Python Bindings Architecture

## Decision: PyO3/maturin with workspace subcrate

Added `python/` as a workspace member (`murr-python` Rust crate, `murr` PyPI package).

## Key design choices

- **Workspace, not cdylib in main crate**: Keeps PyO3 deps isolated from the server binary. The `python/` crate depends on `murr` by path.
- **Sync Python API with Rust-side blocking**: Each `PyLocalMurr` owns a `tokio::runtime::Runtime`. All async `MurrService` methods are called via `runtime.block_on()`. No Python-side async.
- **Schema passing via PyO3 FromPyObject/IntoPyObject**: Newtype wrappers (`PyDType`, `PyColumnSchema`, `PyTableSchema`) in `python/src/schema.rs` implement `FromPyObject` and `IntoPyObject`. Supports both Pydantic models and plain dicts (getattr with get_item fallback). Returns plain dicts on Rust→Python path; Python client wraps with `model_validate()`. Previously used JSON bridge (`serde_json`), replaced for cleaner API.
- **Arrow zero-copy via C Data Interface**: `arrow::pyarrow::{FromPyArrow, ToPyArrow}` for RecordBatch. Uses Arrow FFI — pointer exchange, no serialization.
- **Error mapping via `into_py_err` function**: Can't use `From<MurrError> for PyErr` due to orphan rule (neither type is local). Used a conversion function instead.
- **Pydantic v2 for validation**: `DType(str, Enum)`, `ColumnSchema(BaseModel)`, `TableSchema(BaseModel)`. No `name` field on `TableSchema` — matches Rust design (PR #27).
- **PyO3 0.28**: Required by `arrow 58`'s `pyarrow` feature (arrow-pyarrow depends on pyo3 0.28).
- **Target Python 3.14**: CI matrix build added for Python 3.13 and 3.14 (`python-test` job in `ci.yml`).
