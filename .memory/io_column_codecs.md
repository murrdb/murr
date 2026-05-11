# io column encoder/decoder design

## Shape

`io::column` exposes two narrow trait pairs and free factory functions:

```rust
pub trait ColumnEncoder: Send {
    fn add_row(&mut self, row: &ReadRow) -> Result<(), MurrError>;
    fn build(&mut self) -> ArrayRef;
}
pub trait ColumnDecoder: Send + Sync {
    fn write_to_row(&self, index: usize, row: &mut WriteRow) -> Result<(), MurrError>;
}
pub fn encoder_for(column: &SegmentColumnSchema, rows: usize) -> Box<dyn ColumnEncoder>;
pub fn decoder_for(column: &SegmentColumnSchema, array: &dyn Array)
    -> Result<Box<dyn ColumnDecoder>, MurrError>;
```

Concrete impls: `PrimitiveEncoder<T>` / `PrimitiveDecoder<T>` (any `ArrowPrimitiveType` whose `Native: Pod`/`NoUninit` — Float32 and Float64 today) and `Utf8Encoder` / `Utf8Decoder`.

## Why split into per-column encoder + decoder objects (not one Codec)

The old `io::column::ColumnCodec` had one trait with `encode(array, &mut [Row])` and `decode(&[Row]) -> ArrayRef`. That bounces dtype dispatch on every call and forces the caller to materialize a row buffer up front.

The io split is row-oriented:
- **read path**: pre-create one `ColumnEncoder` per requested column, then loop rows from the KV store; each row hits each encoder once. Row stays in L1; column builders are pre-allocated. Adding a dtype = one match arm in `encoder_for`.
- **write path**: pre-create one `ColumnDecoder` per source array, then loop row indices; each row visits each decoder once.

## Why row-index addressing instead of an internal cursor

`write_to_row(&self, index: usize, ...)` is `&self`, not `&mut self`. The caller owns the loop, decoders are stateless and `Sync`-safe. An internal-cursor design (`&mut self`) would couple decoder progress to caller iteration order and disallow `Sync`.

## Why the traits dropped `schema()` / `column()` accessors

The earlier draft had `fn schema()` / `fn column()` accessors on both traits. Callers always already own the `SegmentSchema` and `SegmentColumnSchema` (they had to, to *construct* the encoder). Owning a clone inside each impl just to satisfy the accessor adds noise without a real consumer.

## Why free factory functions, not `impl dyn Trait`

`encoder_for` / `decoder_for` are free functions in `io::column`. The `<dyn Trait>::factory(...)` pattern was considered but rejected as visually ugly at call sites. Free functions land at `column::encoder_for(&col, n)` after a `use crate::io::column;` — same dtype-dispatch role, cleaner read.

## Why per-impl `new` is a private inherent method, not a trait method

A `fn new(...) -> Self` on the trait would require `Self: Sized` (excluding it from the vtable) and force callers to know the concrete type, defeating dynamic dispatch. The factory is the single dtype-aware constructor; `PrimitiveEncoder::new` etc. exist as private inherent constructors only used by the factory.

## Cost notes

`PrimitiveDecoder::new` / `Utf8Decoder::new` clone the typed array (`PrimitiveArray<T>::clone` / `StringArray::clone`). These are **buffer-Arc bumps, not data copies** — same cost as cloning the `ArrayRef` itself. The clone happens once per decoder construction, keeping `write_to_row` free of repeat downcasts on the hot path.

`Utf8Encoder` validates UTF-8 once per row via `std::str::from_utf8` and surfaces invalid bytes as `MurrError::SegmentError`. The validation lives in the encoder (read path) rather than in `ReadRow::read_dynamic`, which keeps the row helper byte-level and reusable for future `Binary` / `List` codecs.
