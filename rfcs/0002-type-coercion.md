# Type Coercion

Status: Implemented

Authors:

* [Roman Grebennikov](https://github.com/shuttie)

## Summary

Murr casts an Arrow column to the dtype of the table schema, on writes and for
the keys of a fetch request. There are two kinds of casts:

- Widening casts, which cannot change a value. They work on every column.
- Rounding of `float64` into `float32`. It needs `"strict": false` on the
  column. The default is `"strict": true`.

An integer is never narrowed.

## Motivation

Today an Arrow array must have exactly the dtype from the schema.

```python
pa.table({"id": ["a", "b"], "score": [0.1, 0.2]})   # score is float64
```

A write of this table into a `float32` column fails with
`expected Float32, got Float64`. The usual clients produce such types:

| Client  | Sends                   | Schema usually has |
|---------|-------------------------|--------------------|
| pyarrow | `float64`               | `float32`          |
| polars  | `LargeUtf8`, `Utf8View` | `utf8`             |

RFC 0001 left the cast to the client bindings. The Python client does not cast,
and a user of plain HTTP with Arrow IPC or Parquet has no binding at all.

## Goals

- A batch with a narrower type than the schema works on every column.
- A `float64` batch can be written to a `float32` column after an opt-in.
- A batch that already has the schema types costs the same as today.

## Non-Goals

- Integer narrowing, `int64` into `int32` for example. See Alternatives.
- Float to integer casts.
- Dictionary-encoded strings (pandas categoricals).
- Casts between strings and numbers, and casts from or into `bool`.
- Changing `strict` on an existing column. Drop the table and create it again.

## Design

### Schema

```json
{
  "columns": {
    "id":    {"dtype": "int64", "key": true, "nullable": false},
    "score": {"dtype": "float32", "strict": false}
  }
}
```

`strict` is optional and defaults to `true`. It is stored in `manifest.json`
with the rest of the column.

### Widening casts, on every column

The rule: every value of the source type has an exact form in the column dtype.
Only the two types are compared, never the values.

| Column dtype | Accepted source types                             |
|--------------|---------------------------------------------------|
| `int16`      | `int8`, `uint8`                                   |
| `int32`      | `int8`, `int16`, `uint8`, `uint16`                |
| `int64`      | `int8` to `int32`, `uint8` to `uint32`            |
| `uint16`     | `uint8`                                           |
| `uint32`     | `uint8`, `uint16`                                 |
| `uint64`     | `uint8` to `uint32`                               |
| `float32`    | `int8`, `int16`, `uint8`, `uint16`                |
| `float64`    | `float32`, `int8` to `int32`, `uint8` to `uint32` |
| `utf8`       | `LargeUtf8`, `Utf8View`                           |

`int32` into `float32` is out, because `float32` is exact only up to 2^24.
`int64` into `float64` is out for the same reason at 2^53.

### Rounding, only with `strict: false`

A `float32` column with `strict: false` also accepts `float64`.

- Each value is rounded to the nearest `float32`. `0.1` is stored as `0.1f32`.
- A value outside the `float32` range, `1e300` for example, is stored as
  infinity. This is what the Arrow cast returns. A check would need one more
  pass over each batch, and a user who sets `strict: false` accepts the loss.
- `strict: false` on a column of any other dtype is accepted and changes
  nothing.

### Keys

Key columns get the widening casts, on writes and in fetch requests.

- Keys are `utf8` or integers, so `strict` has no effect on a key column.
- The cast runs before key encoding. Key bytes are zigzag varints for signed
  types, so `5` has different bytes as `uint32` and as `int64`.

### Where the cast happens

Each dtype declares the Arrow types that it accepts, in two methods of the
`DType` trait: `widens_from` and `rounds_from`. One struct in `io`, `Coercion`,
loops over the columns of a batch and casts the mismatched ones. `Table` builds
it from the schema.

```
Table::write -+
              +-> coercion -> KeySchema, codecs
Table::read  -+
```

- The codecs and the key encoders do not change and stay exact.
- The cast is `arrow::compute::cast_with_options`, behind our own list of type
  pairs. Arrow can cast more than we want, a string into a number for example.
- A column that already matches is passed on as the same `Arc`.

### What does not change

- Responses always have the schema dtype.
- A type pair outside the rules above is a 400.
- JSON requests. They are parsed by the schema dtype, so `0.1` for a `float32`
  column is rounded already, on strict columns too.

## Compatibility

- `manifest.json`: old files load, and all their columns are strict. An older
  murr binary rejects a manifest that has the `strict` field.
- HTTP API and `openapi.yaml`: `ColumnSchema` gets `strict`, in table creation
  and in the schema endpoint. Some requests that were a 400 now succeed.
- Flight: `do_get` takes keys as JSON and is not affected.
- Python client: `ColumnSchema` needs the `strict` field.
- RFC 0001: the "Strict key types" section gets an Updates line that points
  here.

## Performance

A matching batch costs one type comparison per column. A mismatched column
costs one pass and one new array. I have not run the benchmarks with a
mismatched batch.

## Testing

- `Table` on `MemoryStore`: an `rstest` table of (source array, column dtype,
  `strict`) with the array that reads back, or none for a rejected write. It
  has pairs from the widening table, `float64` into `float32` with both values
  of `strict` (`0.1` and `1e300` read back as `0.1f32` and infinity), and three
  rejected pairs: `int64` into `int32` and `int32` into `float32` with
  `strict: false`, and `utf8` into `int64`.
- A row written with a `uint32` key into an `int64` key column is found by an
  `int32` fetch key.
- HTTP: a `float64` IPC write is a 400 on a strict column and works on a
  column with `"strict": false`, and the schema endpoint returns the flag.

## Alternatives

| Alternative | Good | Why it lost |
|---|---|---|
| Cast in the client (RFC 0001) | Simple server. | Each client and each plain HTTP user needs the same cast table. |
| Strict means no casts at all | Easiest rule to explain. | A default column still rejects `int32` for `int64`, where the user has nothing to decide. |
| Integer narrowing with `strict: false` | pyarrow infers `int64` for every Python int. | A float rounds, an integer overflows. The batch works until one large id arrives, and then the write fails or the value is wrong. |
| Reject floats that do not round-trip | Lossless, no flag. | `0.1` fails, and so do most real feature values. |
| One flag per table | One line in the schema. | Rounding is a decision about one column. A table flag opens it for all of them. |
| Cast inside the codecs | No copy of the column. | Each codec needs a decoder and a key encoder per source type. |
| Cast in the API layer | `io` stays exact. | Each entry point must remember the call, and embedded `Table` users get no cast. |

## Open Questions

- Should `strict: false` on a non-`float32` column be a schema error? The RFC
  accepts it, so that a later cast can use the same flag.
