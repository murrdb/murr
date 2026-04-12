# Generic Scalar Column Design

## Decision

Introduced a `ScalarCodec` trait in `src/io/codec.rs` that unifies the type-level information for fixed-size scalar columns: Arrow primitive type, native Rust type (Pod + ArrowNativeType), element size, zero value for nulls, and name. `Float32Codec` is the first implementation.

Generic `ScalarColumnReader<R, S>` and `ScalarColumnWriter<S>` in `src/io/column/scalar/` replace the concrete float32 reader/writer. The float32 module became thin type aliases.

## Key architectural decisions

**ScalarCodec trait vs separate Encoder/Decoder**: A single unified trait is simpler and avoids the need to coordinate separate encoding/decoding traits. All the information a scalar column needs (element size, zero value, arrow type, native type) belongs together.

**ColumnWriter made generic over Array type**: `ColumnWriter<A: Array>` takes a concrete `&A` instead of `Arc<dyn Array>`. The dispatch/downcast happens once in `table/writer.rs::write_column`, matching on `(DType, DataType)` tuple. This eliminates the double-indirection of dyn dispatch + downcast.

**ColumnSegmentBytes redesigned with buffers**: Changed from `bytes: Vec<u8>` to `buffers: Vec<PayloadBytes>` + `footer: Vec<u8>` with `to_bytes()` assembling the final blob. `PayloadBytes` auto-computes 8-byte alignment padding. This makes the buffer structure explicit and enables validation (e.g., scalar columns must have exactly 2 buffers, utf8 must have 3).

**ScalarColumnFooter**: Same binary layout as the old Float32ColumnFooter (payload + bitmap offsets). Renamed and moved to `scalar/footer.rs`. Reused via type alias for float32.

## Why not alternatives

- **Macro-based code generation**: Would duplicate the reader/writer code per type with less type safety. Generics with a trait bound are idiomatic Rust and give compiler-checked type consistency.
- **Keeping ColumnWriter as `Arc<dyn Array>`**: Forces a downcast in every writer impl. Since the table writer already dispatches on dtype, the downcast is redundant. Generic `ColumnWriter<A>` is cleaner.
- **Separate CodecBuffers struct**: Initially considered a separate `CodecBuffers` struct for codec output vs `ColumnSegmentBytes` for the writer. Unified them since the column writer is the only producer and the directory writer is the only consumer.
