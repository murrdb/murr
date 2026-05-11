# io wired into MurrService

`MurrService` drives `io::store::rocksdb::RocksDBStore` directly. There is one `io` module in the codebase — it is the RocksDB-backed store/table layer. (The legacy `.seg`-format `io` that pre-dated this one was deleted on 2026-05-10 along with the parallel `io4` rename; the `Directory`/`Reader`/`Writer` trait stack from that era is gone.)

## Config shape

`StorageConfig { path, backend }`. `BackendConfig` is a serde-externally-tagged enum with two variants — `Mmap(PlainConfig)` and `Block(BlockConfig)` — flattened into `StorageConfig` so the YAML reads:

```yaml
storage:
  path: /var/lib/murr
  mmap: {}        # or `block: {}`
```

Inner keys nest the corresponding RocksDB tunables. Specifying both is rejected by serde; specifying neither falls back to `Mmap(PlainConfig::default())` via `#[serde(flatten, default)]`.

**Why labels `mmap`/`block`, not `memory`/`disk`** — RocksDB layout naming. Both write SSTs + WAL + `manifest.json` to disk; the difference is how SSTs are read. `mmap` = `RocksDBStore::open_plain` (PlainTable, mmap_reads required). `block` = `open_block` (block-based, optional mmap or O_DIRECT).

**Why `path` lives at the storage level, not inside the backend** — same RocksDB DB path either way; nesting it would force users to repeat themselves when toggling backends.

**Why `deny_unknown_fields` was dropped from `StorageConfig`** — `#[serde(flatten)]` over an externally-tagged enum forwards every unknown field to the enum, which conflicts with strict-mode parsing. Inner `PlainConfig`/`BlockConfig` can keep field-level strictness independently.

## Service shape

```rust
pub struct MurrService {
    tables: std::sync::RwLock<HashMap<String, io::table::Table<RocksDBStore>>>,
    store: Arc<std::sync::RwLock<RocksDBStore>>,
    config: Config,
}
```

One `Arc<StdRwLock<RocksDBStore>>` shared across all tables (one RocksDB DB, many CFs). Each table holds its own clone of the `Arc` and locks it internally per call. The service's public methods (`new`, `create`, `write`, `read`, `list_tables`, `get_schema`) are all **sync**.

**Why no `TableOps` trait, no `Box<dyn ...>`** — only one concrete backend exists (`RocksDBStore`), and `io::Table::read`/`write` already take `&self` (the store-level RwLock handles serialisation). Trait erasure plus `&mut self` would have forced HashMap-write-lock during writes; concrete `Table<RocksDBStore>` lets the HashMap stay at a read lock for both reads and writes, so writes on different tables run concurrently. Same-table serialisation comes from the store's internal RwLock.

**Why std::sync::RwLock both inside and outside** — the only async work in the service was the `.await` on the outer tables-registry lock, which guards a HashMap lookup. With RocksDB sync, the entire method body is CPU/blocking work; pretending it's async invited holding std locks across `.await` boundaries. The crossing point now lives one level up at the API handlers (see below).

**Lock poisoning** — service methods recover via `unwrap_or_else(PoisonError::into_inner)` rather than `.expect()`. The inner HashMap can be inconsistent only if a panic landed mid-`insert`; for our use (insert-once + mostly reads) recovery is safe. Inside `io::Table` and `RocksDBStore` the existing pattern still uses `.expect("...lock poisoned")` — pre-existing, unchanged.

## Async/sync boundary at API handlers

HTTP handlers (`api/http/handlers.rs`), Flight RPCs (`api/flight/mod.rs`), and `PyMurrLocalAsync` all wrap service calls in `tokio::task::spawn_blocking(move || ...)`. The Arc-cloned `MurrService` and any owned arguments (`String`, `Vec<String>`, `Bytes`) move into the closure; `&str` slices are rebuilt inside. `JoinError`s (panic-only — the closure itself returns `Result<_, MurrError>`) map to `MurrError::IoError` → HTTP 500 / gRPC Internal.

**Why `spawn_blocking` over `block_in_place`** — the blocking pool is demand-spawned up to 512 threads, so concurrency isn't capped at `worker_threads`. PlainTable+mmap reads are usually short, but cold-cache block reads can take ms; capping in-flight requests at core count would hurt tail latency. The ~1µs thread-hop is negligible against multi-key read cost.

**Why fetch/write-table do their Arrow/Parquet encode/decode inside the closure** — that's CPU work too, and pulling it onto the blocking pool keeps the async runtime free. Only response wrapping (`Json`, `into_response`) stays async.

**Why `PyMurrLocalSync` no longer needs an unconditional tokio Runtime** — without async data ops there's nothing to `block_on`. The Runtime is now `Option<Runtime>`, present only when `http_port` is set (the in-process HTTP server still needs a tokio handle to live on).

**Why the empty-table read returns nulls instead of erroring** — missing-key semantics are uniform: any key the store doesn't have produces `None` in the read result, which the column encoder turns into a null row. A table with zero rows is just the all-keys-miss case. No special case for "table has no data."

## Startup rehydration

On `MurrService::new`, the service:
1. `mkdir -p` the storage path (RocksDB needs the parent dir to exist).
2. Picks `RocksDBStore::open_plain` or `open_block` based on the variant.
3. Snapshots `store.manifest().tables` (clones the schemas), drops the manifest read lock, and constructs an `io::Table::open` per entry. Failures get logged + skipped; the service still comes up.

The manifest is the source of truth for which CFs the service wraps — `RocksDBStore::open_inner` already calls `DB::list_cf` so the underlying CFs are picked up regardless. A CF without a manifest entry is invisible to the service (intentional — keeps schema discovery and CF discovery in lockstep).

## Where helpers landed

`resolve_cache_dir` + `is_dir_writable` live in `src/conf/path.rs` (they were previously colocated with the deleted `.seg`-format `io::directory::mmap` module).
