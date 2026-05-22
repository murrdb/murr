# io::codec design

## Shape

`io::codec` exposes one trait per dtype that owns both Arrow↔row and Arrow↔JSON conversions:

```rust
pub trait Codec: Send + Sync {
    fn dtype(&self) -> DType;
    fn arrow_dtype(&self) -> DataType;

    fn to_json(&self, arr: &dyn Array) -> Result<Vec<Value>, MurrError>;
    fn from_json(&self, vals: &[Value]) -> Result<ArrayRef, MurrError>;

    fn make_encoder(&self, col: SegmentColumnSchema, rows: usize) -> Box<dyn ColumnEncoder>;
    fn make_decoder(&self, col: SegmentColumnSchema, arr: &dyn Array)
        -> Result<Box<dyn ColumnDecoder>, MurrError>;
}

pub fn codec_for(dtype: DType) -> &'static dyn Codec;
```

`ColumnEncoder`/`ColumnDecoder` stay as separate stateful traits — `Codec` is a stateless `&'static` singleton per dtype that hands out fresh encoder/decoder objects for each batch.

One file per supported dtype lives under `src/io/codec/`: `bool_.rs`, `utf8.rs`, and one per numeric type (`int8..int64`, `uint8..uint64`, `float32`, `float64`). Each declares a unit struct (`Int32Codec`, etc.) implementing `Codec`. The 10 numeric files delegate every method to generic helpers in `primitive.rs` (`Encoder<T>`, `Decoder<T>`, `to_json::<T>`, `from_json::<T>`), so each numeric file is ~30 lines of mechanical shape.

`bool_` and `utf8` are self-contained because neither bool nor strings fit the `ArrowPrimitiveType + bytemuck::Pod` shape required by the generic primitive helpers. (`bool_` rather than `bool` to avoid clashing with the primitive type in imports.)

## Why this shape (and not what it replaced)

The previous shape had two split trait families: `io::column::{ColumnEncoder, ColumnDecoder}` + `encoder_for`/`decoder_for` factories for row codec, and `api::http::json::{PrimitiveJsonCodec, JsonCodec}` for JSON codec. Adding a dtype meant editing seven 12-arm match expressions across `core/schema.rs`, `io/column/mod.rs`, and `api/http/convert.rs`. The two trait families also had asymmetric shapes — `PrimitiveJsonCodec` was implemented on the Arrow type marker (`Int8Type`), `JsonCodec` on the Rust scalar type (`String`, `bool`) — forced by Rust's coherence rules (blanket impl on `ArrowPrimitiveType` overlaps any concrete impl).

The `Codec` trait collapses both responsibilities behind one dtype-keyed dispatcher. Adding a dtype now means: one variant in `DType` enum, one arm each in `DType::size` / `From<&DType> for DataType` / `TryFrom<&DataType> for DType` (the dtype-set declaration in `core/schema.rs`), one arm in `codec_for`, and one new file in `src/io/codec/`. The seven match expressions in callers collapse to a single `codec_for(dtype).method(...)` call.

## Why stateful Encoder/Decoder traits remain

`Codec::make_encoder` returns `Box<dyn ColumnEncoder>` rather than `Codec` itself holding the builder. This is because:

- The read path pre-creates one encoder per requested column, then loops rows from the KV store; each row hits each encoder once. The builder must accumulate across rows.
- The write path pre-creates one decoder per source array, then loops row indices.
- A stateful `Codec` would force fresh allocation per batch and prevent `&'static dyn Codec` singletons.

`ColumnDecoder::write_to_row(&self, ...)` takes `&self`, not `&mut self` — decoders are stateless and `Sync`. The caller owns the loop. An internal-cursor design (`&mut self`) would couple decoder progress to caller iteration order and disallow `Sync`.

## Why core/schema.rs is left as-is

The three 12-arm matches there (`DType::size`, `From<&DType> for DataType`, `TryFrom<&DataType> for DType`) are already one-liners per arm and conceptually *declare* the dtype set itself. Folding them into `Codec` would require runtime dispatch (`codec_for(dt).arrow_dtype()`) for what should be compile-time constants — these matches are the source of truth `codec_for` indexes into.

## Cost notes

`primitive::Decoder::new` / `Utf8Decoder` / `BoolDecoder` clone the typed array (`PrimitiveArray<T>::clone` / `StringArray::clone` / `BooleanArray::clone`). These are **buffer-Arc bumps, not data copies** — same cost as cloning the `ArrayRef` itself. The clone happens once per decoder construction, keeping `write_to_row` free of repeat downcasts on the hot path.

`Utf8` encoder validates UTF-8 once per row via `std::str::from_utf8` and surfaces invalid bytes as `MurrError::SegmentError`. The validation lives in the encoder (read path) rather than in `ReadRow::read_dynamic`, which keeps the row helper byte-level and reusable for future `Binary` / `List` codecs.

`primitive::from_json` calls `serde_json::from_value(v.clone())` — the clone is required because `from_value` consumes `Value`. Pre-clone overhead is bounded by `Value::Number` size (one heap word for the common case); avoided cost is the cost of writing a borrowed-deserializer path, which is not worth it for the JSON write throughput.

## Supported dtypes

Concrete codecs today: `Int8/16/32/64Codec`, `UInt8/16/32/64Codec`, `Float32/64Codec` (delegate to `primitive`), plus self-contained `BoolCodec` and `Utf8Codec`. Bool stores as 1-byte 0/1 in the static row section and packs into `BooleanBuilder`/`BooleanArray` at the Arrow boundary. Float16 was considered and skipped: `half::f16`'s serde impl serializes as `newtype_struct(u16)` of raw bits, so JSON would surface `1.5` as `15872` — not worth the API ugliness for the niche dtype.
