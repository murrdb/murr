# io::codec design

## Shape

`core` declares the dtype name (a serde-friendly enum) and the typed-view trait, kept narrow:

```rust
pub enum DTypeName { Utf8, Bool, Int8..Int64, UInt8..UInt64, Float32, Float64 }
pub trait DType: Send + Sync + 'static {
    fn name(&self) -> DTypeName;
    fn arrow_dtype(&self) -> DataType;
    fn size(&self) -> usize;
}
```

`io::codec` splits the storage-vs-wire codecs into two traits, plus a join trait `Codec` that exposes all three behind one dispatcher:

```rust
pub trait ArrowCodec: Send + Sync {
    fn make_encoder(&self, col: SegmentColumnSchema, rows: usize) -> Box<dyn ColumnEncoder>;
    fn make_decoder(&self, col: SegmentColumnSchema, arr: &dyn Array)
        -> Result<Box<dyn ColumnDecoder>, MurrError>;
}
pub trait JsonCodec: Send + Sync {
    fn to_json(&self, arr: &dyn Array) -> Result<Vec<Value>, MurrError>;
    fn from_json(&self, vals: &[Value]) -> Result<ArrayRef, MurrError>;
}

pub trait Codec: DType + ArrowCodec + JsonCodec {}
impl<T: DType + ArrowCodec + JsonCodec> Codec for T {}

impl DTypeName {
    pub fn codec(self) -> Box<dyn Codec> { match self { ... } }
}
```

The single `DTypeName::codec()` dispatcher returns a `Box<dyn Codec>` from which all `DType` / `ArrowCodec` / `JsonCodec` methods are reachable (the supertraits' methods sit in the same vtable). Callers always go through `name.codec()`, never directly through the per-type structs.

One file per supported dtype lives under `src/io/codec/`: `bool_.rs`, `utf8.rs`, and one per numeric type. Each declares a unit struct (`Int32`, `Float32`, `Bool`, `Utf8`, ...) and hangs three impl blocks off it: `DType`, `ArrowCodec`, `JsonCodec`. The blanket impl picks them up as `Codec` automatically. The 10 numeric files delegate the codec methods to generic helpers in `primitive.rs` (`Encoder<T>`, `Decoder<T>`, `to_json::<T>`, `from_json::<T>`), so each is ~50 lines of mechanical shape.

`ColumnEncoder`/`ColumnDecoder` remain separate stateful traits returned by `make_encoder` / `make_decoder` (see "Why stateful Encoder/Decoder traits remain" below).

`bool_` and `utf8` are self-contained because neither bool nor strings fit the `ArrowPrimitiveType + bytemuck::Pod` shape required by the generic primitive helpers. (`bool_` rather than `bool` to avoid clashing with the primitive type in imports.)

## Why this shape (and not what it replaced)

The original `io::column` shape had two split trait families: `io::column::{ColumnEncoder, ColumnDecoder}` + `encoder_for`/`decoder_for` factories for row codec, and `api::http::json::{PrimitiveJsonCodec, JsonCodec}` for JSON codec. Adding a dtype meant editing seven 12-arm match expressions across `core/schema.rs`, `io/column/mod.rs`, and `api/http/convert.rs`.

The intermediate `Codec` trait collapsed both responsibilities into one. That worked, but conflated two concerns: JSON conversion (used only by the REST layer) and Arrow row/column conversion (used only by storage). It also kept ad-hoc 12-arm matches in `core/schema.rs` (`DType::size`, `From<&DType> for DataType`, `TryFrom<&DataType> for DType`), so the source of truth for dtype properties was spread across two files.

The current shape moves the property source of truth onto the per-type struct via the `DType` trait, then splits the per-row/per-batch behavior into `ArrowCodec` and `JsonCodec`. Adding a dtype now means: one variant in `DTypeName` enum, one arm in `TryFrom<&DataType> for DTypeName` (`io/schema.rs`), one new file in `src/io/codec/` containing the unit struct and its three impl blocks, and one arm in the single `DTypeName::codec()` dispatcher match. There is no longer any need to edit `core/schema.rs` to add a property.

## Why three traits and one join trait

- `DType` is layer-agnostic metadata (name, Arrow data type, byte size in the row). Anyone — including future non-REST, non-Arrow consumers — can ask about a dtype without pulling in `serde_json` or `ColumnEncoder`/`Decoder`.
- `JsonCodec` is used only by `api/http/convert.rs` (the REST translation layer). Keeping it as a separate trait makes the JSON capability explicit at impl sites.
- `ArrowCodec` is used only by `io/table` and `io/row/read` (the storage path). Same rationale.
- `Codec: DType + ArrowCodec + JsonCodec` collapses the three trait objects callers would otherwise juggle. The blanket impl means every per-type struct that satisfies the three base traits is automatically a `Codec` — no manual wiring per dtype. The original split is preserved at the trait declarations and per-type impl blocks; only the *dispatch surface* is unified.

Future parametrized variants like `Time32 { unit: TimeUnit }` slot in cleanly: the struct gets a field, the per-type file grows by one impl block per parameter case if needed, and `DTypeName::codec()` gains one match arm. No central match in `core/schema.rs` needs updating because the property declarations live on the struct, not on the enum.

## Why `Box<dyn …>` and not `&'static dyn …`

All current per-type structs are ZSTs, so `&'static dyn DType` would work. The dispatchers return `Box<dyn …>` anyway because the day parametrized variants like `Time32 { unit: TimeUnit }` arrive, the struct will carry data and can no longer all be `'static`. Switching the dispatcher signature then would be a breaking change across every call site; eating the small `Box` cost today buys us a stable API. The dispatchers are called per-batch per-column (not per-row), so the allocation cost is sub-microsecond per call and not on the hot iteration loop.

## Why the arrow glue lives in `io/schema.rs`, not `core/schema.rs`

`From<&TableSchema> for Schema` and `TryFrom<&DataType> for DTypeName` need to call `dtype_for(name).arrow_dtype()`. The dispatcher lives in `io/codec/mod.rs`. If these `From` impls stayed in `core`, `core` would have to depend on `io/codec` — the wrong dependency direction (core is the bottom layer). All current callers of these impls (`api/flight/mod.rs`, `api/http/convert.rs`, `io/table/mod.rs`, `io/row/read.rs`) already sit at or above the `io` layer, so the relocation does not break any caller's dependency direction.

## Why stateful Encoder/Decoder traits remain

`ArrowCodec::make_encoder` returns `Box<dyn ColumnEncoder>` rather than `ArrowCodec` itself holding the builder. This is because:

- The read path pre-creates one encoder per requested column, then loops rows from the KV store; each row hits each encoder once. The builder must accumulate across rows.
- The write path pre-creates one decoder per source array, then loops row indices.
- A stateful `ArrowCodec` would force fresh allocation per batch and prevent ZST codec impls.

`ColumnDecoder::write_to_row(&self, ...)` takes `&self`, not `&mut self` — decoders are stateless and `Sync`. The caller owns the loop. An internal-cursor design (`&mut self`) would couple decoder progress to caller iteration order and disallow `Sync`.

## Cost notes

`primitive::Decoder::new` / `Utf8Decoder` / `BoolDecoder` clone the typed array (`PrimitiveArray<T>::clone` / `StringArray::clone` / `BooleanArray::clone`). These are **buffer-Arc bumps, not data copies** — same cost as cloning the `ArrayRef` itself. The clone happens once per decoder construction, keeping `write_to_row` free of repeat downcasts on the hot path.

`Utf8` encoder validates UTF-8 once per row via `std::str::from_utf8` and surfaces invalid bytes as `MurrError::SegmentError`. The validation lives in the encoder (read path) rather than in `ReadRow::read_dynamic`, which keeps the row helper byte-level and reusable for future `Binary` / `List` codecs.

`primitive::from_json` calls `serde_json::from_value(v.clone())` — the clone is required because `from_value` consumes `Value`. Pre-clone overhead is bounded by `Value::Number` size (one heap word for the common case); avoided cost is the cost of writing a borrowed-deserializer path, which is not worth it for the JSON write throughput.

## Supported dtypes

Concrete per-type structs today: `Int8/16/32/64`, `UInt8/16/32/64`, `Float32/64` (delegate to `primitive`), plus self-contained `Bool` and `Utf8`. Each implements all three traits (`DType`, `ArrowCodec`, `JsonCodec`). Bool stores as 1-byte 0/1 in the static row section and packs into `BooleanBuilder`/`BooleanArray` at the Arrow boundary. Float16 was considered and skipped: `half::f16`'s serde impl serializes as `newtype_struct(u16)` of raw bits, so JSON would surface `1.5` as `15872` — not worth the API ugliness for the niche dtype.
