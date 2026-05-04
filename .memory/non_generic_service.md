# Non-generic MurrService via Box<dyn TableOps>

## Decision

`MurrService` and the API layers (`MurrHttpService`, `MurrFlightService`) are non-generic. The runtime backend choice (mmap vs mem) is driven by `BackendConfig` in `StorageConfig`.

## How it works

`io::table::Table<D>` is the per-table abstraction (replaces the old `service::TableState<D>`):
- Owns `Arc<D>` and `Option<TableReader<D::ReaderType>>`
- `create(index, schema, config)` / `open(index, config)` constructors
- Internally handles writer-open + write + reader-reopen on each `write()` call

`io::table::TableOps` is the object-safe trait that erases `D`:
```rust
#[async_trait]
pub trait TableOps: Send + Sync {
    fn schema(&self) -> &TableSchema;
    async fn read(&self, keys: &[&str], cols: &[&str]) -> Result<RecordBatch, MurrError>;
    async fn write(&mut self, batch: &RecordBatch) -> Result<(), MurrError>;
}

impl<D: Directory> TableOps for Table<D> { ... }
```

`MurrService` stores `RwLock<HashMap<String, Box<dyn TableOps>>>` and matches on `config.storage.backend` in `new()` / `create()` to construct the concrete `Table<MMapDirectory>` or `Table<MemDirectory>` and box it.

## Why this shape (rejected alternatives)

- **`MurrService<D>` generic, exposed publicly**: leaks `D` into HTTP/Flight API types and forces every caller to pick a backend at compile time. This was the original design.
- **`TableHandle` trait inside `service/`**: looked like a "service-level wrapper that re-declares the service API". The trait belongs in `io/table/` because it IS the per-table interface — `Table<D>` is its only impl.
- **`AnyTable` enum at service level**: explicit enum dispatch with `match` arms for every method — same API surface duplicated a third time (after `Table<D>` and `MurrService`). Rejected as visual noise.
- **`AnyDirectory` enum implementing `Directory` + `type MurrService = MurrServiceGeneric<AnyDirectory>`**: requires implementing `Directory` + `DirectoryReader` + `DirectoryWriter` for three new wrapper enums (~150 lines of mechanical match-and-delegate boilerplate in `io/directory/`). More boilerplate than `Box<dyn TableOps>`.

## Trait changes that fell out

`Directory::create / open / list_indexes` no longer take a separate `&Self::Location` argument — the location lives inside `Self::ConfigType` if the backend needs one. `MMapConfig` carries `cache_dir: PathBuf`; `MemConfig` is empty. `type Location: Url` was removed from `Directory`, and `src/io/url.rs` (the `Url` marker trait + `LocalUrl`/`MemUrl`/`S3Url` wrappers) was deleted entirely. `MMapDirectory` stores `root: PathBuf` directly.

## Write lock during writes

`TableOps::write` takes `&mut self` because each write reopens the reader. `MurrService::write()` holds the HashMap write lock for the duration of `table.write(batch).await`. This is the same concurrency model as before the refactor (reads do not block on writes via a separate read guard path, but two writes serialize) — not a regression.
