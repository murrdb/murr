# io4 wired into MurrService

`MurrService` now drives `io4::store::rocksdb::RocksDBStore` directly. The old `io::table::Table<D>` + `Box<dyn TableOps>` shape is gone from the service. The `io` module itself stays compiled but unused by the service (slated for removal).

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
    tables: tokio::sync::RwLock<HashMap<String, io4::table::Table<RocksDBStore>>>,
    store: Arc<std::sync::RwLock<RocksDBStore>>,
    config: Config,
}
```

One `Arc<StdRwLock<RocksDBStore>>` shared across all tables (one RocksDB DB, many CFs — the io4 model). Each table holds its own clone of the `Arc` and locks it internally per call.

**Why no `TableOps` trait, no `Box<dyn ...>`** — only one concrete backend exists (`RocksDBStore`), and `io4::Table::read`/`write` already take `&self` (the store-level RwLock handles serialisation). Trait erasure plus `&mut self` would have forced HashMap-write-lock during writes; concrete `Table<RocksDBStore>` lets the HashMap stay at a read lock for both reads and writes, so writes on different tables run concurrently. Same-table serialisation comes from the store's internal RwLock.

**Why `tokio::sync::RwLock` outside, `std::sync::RwLock` inside** — outer lock is contended only briefly (HashMap insert/lookup) but crosses `.await` boundaries; tokio version is the right choice. Inner lock wraps `RocksDBStore` whose ops are all sync — std lock is cheaper and `io4::Table` already expects `Arc<RwLock<S>>` shape from `std`.

**Why the empty-table read returns nulls instead of erroring** — io4's missing-key semantics are uniform: any key the store doesn't have produces `None` in the read result, which the column encoder turns into a null row. A table with zero rows is just the all-keys-miss case. The earlier `MurrError::TableError("table has no data")` short-circuit was specific to `io::table::TableReader`'s "no segments yet" state and didn't translate to a per-key store. Aligning on null-batch removes the special case.

## Startup rehydration

On `MurrService::new`, the service:
1. `mkdir -p` the storage path (RocksDB needs the parent dir to exist).
2. Picks `RocksDBStore::open_plain` or `open_block` based on the variant.
3. Snapshots `store.manifest().tables` (clones the schemas), drops the manifest read lock, and constructs an `io4::Table::open` per entry. Failures get logged + skipped; the service still comes up.

The manifest is the source of truth for which CFs the service wraps — `RocksDBStore::open_inner` already calls `DB::list_cf` so the underlying CFs are picked up regardless. A CF without a manifest entry is invisible to the service (intentional — keeps schema discovery and CF discovery in lockstep).

## Where helpers landed

`resolve_cache_dir` + `is_dir_writable` moved from `src/io/directory/mmap/directory.rs` into `src/conf/path.rs`. `MMapConfig` now imports the helper from `conf` — that's an `io → conf` dependency direction, which is fine since `conf` is the higher-level module. When `io` is removed, the helper stays put.
