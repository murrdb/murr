# io4::table::Table

`Table<S: Store>` is the glue between Arrow `RecordBatch` (public API) and the byte-oriented `io4::store::Store` (RocksDB CF per table). It's the unit `MurrService` will hold in its registry.

## Shape

```rust
pub struct Table<S: Store> {
    store: Arc<RwLock<S>>,
    name: String,
    table: TableSchema,
    segment: SegmentSchema,            // non-key columns only
    columns: HashMap<String, usize>,   // non-key column name -> index in segment.columns
}

impl<S: Store> Table<S> {
    pub fn create(store, name, table) -> Result<Self>;  // store.create_table(name) + build
    pub fn open(store, name, table)   -> Result<Self>;  // build only; CF assumed to exist
    pub fn read(&self, keys: &[&str], columns: &[&str]) -> Result<RecordBatch>;
    pub fn write(&self, batch: &RecordBatch) -> Result<()>;
}
```

Both `read` and `write` take `&self` — the lock lives inside `Arc<RwLock<S>>`, not on `Table`.

## Why the key column is not encoded into the row payload

Calls always pass keys explicitly (`read(keys, ...)` and `write` extracts them from the batch's key column). Round-tripping the key inside the row blob would just duplicate request data on every read. So `From<&TableSchema> for SegmentSchema` (in `io4::schema`) **filters out the column whose name matches `schema.key`** before assigning bitset indices and offsets. Consequence: requesting the key column via `read(_, &["id"])` returns `MurrError::SegmentError("column 'id' not found")` — the key is lookup-only, not data.

## Why `Arc<RwLock<S>>` instead of owned `S` or interior-mutable `Store`

The `Store` API is multi-table — one CF per table name — so `MurrService` will hold one `PlainRocksDBStore` and many `Table`s sharing it. `Arc<RwLock<S>>` is the simplest composition that works without modifying the `Store` trait (`create_table` / `write` keep their `&mut self` signatures, which compose with `RwLock::write`). Reads acquire a shared read lock so concurrent reads across tables don't block each other.

Alternative considered and rejected: change `Store::write` / `Store::create_table` to `&self` with internal `Mutex` in `PlainRocksDBStore`. That works — RocksDB's `DB::write_opt` is already `&self`, and `DB::create_cf` could be wrapped — but it pushes synchronization into every concrete `Store`, whereas `Arc<RwLock<S>>` keeps it at the call site where it belongs.

## Why `WriteRow.key` is owned `Vec<u8>` (and `WriteRow::new` takes `key: &str`)

`Store::write` takes `rows: impl IntoIterator<Item = (&'a [u8], &'a [u8])>` — borrowed key/value pairs. Source keys come from the input batch's `StringArray`, which only lives for the duration of `Table::write`. Easiest path: each `WriteRow` owns a copied `Vec<u8>` of its key, and `Table::write` hands the store a `rows.iter().map(|r| (r.key.as_slice(), r.bytes.as_slice()))`. The copy is `key.as_bytes().to_vec()` per row — cheap relative to the row payload (which is already a `Vec<u8>`).

`WriteRow::new(schema, key: &str)` takes the key as `&str` (matches the supported `DType::Utf8` key constraint). Tests that don't care about the key pass `""`.

## Why `ColumnEncoder::add_empty()` for missing keys

Earlier draft synthesized one all-null row buffer (`WriteRow::new(&segment, "").bytes`) and fed it through `add_row` for every miss. Cleaner: `ColumnEncoder` exposes `add_empty()` directly. Each impl just calls `builder.append_null()` — one branch instead of bitset-decode-then-null. Used in `Table::read` when `Store::ReadResult::bytes()` yields `None` for a slot.

## Constraints today

- Keys must be `DType::Utf8`. `Table::create`/`Table::open` reject other dtypes with `MurrError::TableError`.
- Output batch fields are always emitted as `nullable=true` regardless of schema nullability — missing keys produce nulls even for non-nullable columns, so the runtime type has to allow it.
